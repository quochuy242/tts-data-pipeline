import os
import shutil
import subprocess
from collections import defaultdict
from typing import List

import numpy as np
import pandas as pd
from loguru import logger
from mutagen.mp3 import MP3
from tqdm import tqdm

from tts_data_pipeline import constants

logger.remove()
logger.add(
  f"{constants.LOG_DIR}/audio_processing.log",
  level="INFO",
  rotation="10 MB",
  encoding="utf-8",
  format=constants.FORMAT_LOG,
  colorize=False,
  diagnose=True,
  enqueue=True,
)


def check_ffmpeg():
  if not shutil.which("ffmpeg"):
    logger.error("ffmpeg is not installed or not in PATH. Please install ffmpeg first.")
    return False

  return True


def convert_mp3_to_wav(mp3_path: str, wav_path: str) -> bool:
  """
  Convert an MP3 file to WAV format using ffmpeg.

  Args:
      mp3_path (str): Path to the MP3 file
      wav_path (str): Path where the WAV file will be saved

  Returns:
      bool: True if conversion is successful, False otherwise
  """
  # Check if ffmpeg is installed
  if not check_ffmpeg():
    return False

  # Convert MP3 to WAV by ffmpeg
  try:
    subprocess.run(
      ["ffmpeg", "-y", "-i", mp3_path, wav_path],
      check=True,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
    )
    return True
  except subprocess.CalledProcessError as e:
    logger.error(f"ffmpeg failed for {mp3_path}: {e}")
    return False
  except Exception as e:
    logger.error(f"Error converting {mp3_path} to WAV: {e}")
    return False


def get_sample_rate(mp3_path: str) -> int:
  """
  Get the sample rate of an MP3 file.

  Args:
      mp3_path (str): Path to the MP3 file.

  Returns:
      int: Sample rate in Hz, or None if there was an error.
  """
  try:
    mp3 = MP3(mp3_path)
    return mp3.info.sample_rate
  except Exception as e:
    logger.error(f"Error getting sample rate for {mp3_path}: {e}")
    return 0


def group_audiobook(mp3_dir: str, unqualified_dir: str) -> List[List[str]]:
  """Efficiently group all parts of audiobooks based on file name prefix.

  Args:
      mp3_dir (str): Path to directory containing mp3 files.
      unqualified_dir (str): Directory to move unqualified files

  Returns:
      List[List[str]]: List of lists, where each sublist contains mp3 file paths of the same audiobook.
  """
  grouped = defaultdict(list)

  for mp3_file in os.listdir(mp3_dir):
    file_path = os.path.join(mp3_dir, mp3_file)
    if not mp3_file.endswith(".mp3"):
      logger.warning(
        f"File {mp3_file} is not an MP3 file, move it to {unqualified_dir}"
      )
      shutil.move(file_path, unqualified_dir)
      continue
    book_name = mp3_file.split("_")[0]
    grouped[book_name].append(file_path)

  return [sorted(files) for files in grouped.values()]


def combine_wav_files(output_path: str, input_paths: List[str]):
  """
  Combine multiple WAV files into a single WAV file using ffmpeg concat demuxer.

  Args:
      output_path (str): Path to save the combined WAV file.
      input_paths (List[str]): List of WAV file paths to combine.
  """
  if not check_ffmpeg():
    return

  if not input_paths:
    logger.warning("No input WAV files provided for combination.")
    return

  try:
    # Create a temporary text file listing input WAV files
    list_file = os.path.join(os.path.dirname(output_path), "input_list.txt")
    with open(list_file, "w", encoding="utf-8") as f:
      for path in input_paths:
        f.write(f"file '{os.path.abspath(path)}'\n")

    # Use ffmpeg with concat demuxer
    subprocess.run(
      [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        list_file,
        "-c",
        "copy",
        output_path,
      ],
      check=True,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
    )

    # Clean up the temporary list file
    os.remove(list_file)

    # Remove the input WAV files
    [os.remove(path) for path in input_paths]
    logger.info(f"Removed input WAV files: {', '.join(input_paths)}")

    logger.info(
      f"Successfully combined {len(input_paths)} WAV files into {output_path}"
    )

  except subprocess.CalledProcessError as e:
    logger.error(f"ffmpeg failed to combine files: {e}")
  except Exception as e:
    logger.error(f"Unexpected error combining WAV files: {e}")


def process_audio_files(
  mp3_dir: str,
  qualified_dir: str,
  unqualified_dir: str,
):
  """
  Process all MP3 files in a directory:
  1. Group all parts of an audiobook together
  2. Convert them to WAV format
  3. Check their sample rate
  4. Move those with sample rates below the threshold to unqualified folder

  Args:
      mp3_dir (str): Directory containing MP3 files
      qualified_dir (str): Directory to save converted WAV files
      unqualified_dir (str): Directory to move unqualified files
  """
  # Create output directories if they don't exist
  os.makedirs(qualified_dir, exist_ok=True)
  os.makedirs(unqualified_dir, exist_ok=True)

  # Read the metadata file for updating sample rate
  metadata_df = pd.read_csv(constants.METADATA_BOOK_PATH)
  metadata_df["sample_rate"], metadata_df["qualified"] = (
    pd.Series([None] * len(metadata_df)),  # A new column for sample rate
    pd.Series([1] * len(metadata_df)),  # A new column for qualified
  )

  # Get all MP3 file paths in the audio directory
  audiobooks = group_audiobook(mp3_dir, unqualified_dir)
  if not audiobooks:
    logger.warning(f"No MP3 files found in {mp3_dir}")
    return

  # Process each MP3 file
  qualified_count = 0
  unqualified_count = 0
  for audiobook in tqdm(audiobooks, desc="Processing audio files"):
    sample_rates = [get_sample_rate(mp3_path) for mp3_path in audiobook]
    if min(sample_rates) < constants.MIN_SAMPLE_RATE:
      unqualified_count += 1
      logger.warning(f"Unqualified book: {audiobook}, sample rates: {sample_rates}")
      # Move unqualified files to unqualified folder
      for mp3_path in audiobook:
        shutil.move(mp3_path, unqualified_dir)
        # Update qualified column for metadata
        metadata_df.loc[
          metadata_df["audio_url"].str.contains(mp3_path.split("/")[-1]),
          "qualified",
        ] = 0
    else:
      qualified_count += 1
      wav_paths = []
      for sample_rate, mp3_path in zip(sample_rates, audiobook):
        # Convert MP3 to WAV
        mp3_filename = mp3_path.split("/")[-1]
        wav_path = qualified_dir + mp3_filename.replace(".mp3", ".wav")
        wav_paths.append(wav_path)
        convert_mp3_to_wav(mp3_path, wav_path)

      # Combine WAV files into a single WAV file
      combined_output_path = os.path.join(
        qualified_dir, wav_paths[0].split("/")[-1].split("_")[0] + ".wav"
      )
      combine_wav_files(combined_output_path, wav_paths)

      # Update sample rate column for metadata
      metadata_df.loc[
        metadata_df["audio_url"].str.contains(mp3_filename), "sample_rate"
      ] = np.mean(sample_rates)

  logger.info(
    f"Processing complete: \n - Total books processed: {len(audiobooks)}\n - Qualified books (≥ {constants.MIN_SAMPLE_RATE} Hz): {qualified_count}\n - Unqualified books (< {constants.MIN_SAMPLE_RATE} Hz): {unqualified_count}"
  )


if __name__ == "__main__":
  logger.info("Test audio processing")

  process_audio_files(
    mp3_dir=constants.AUDIO_RAW_DIR,
    qualified_dir=constants.AUDIO_QUALIFIED_DIR,
    unqualified_dir=constants.AUDIO_UNQUALIFIED_DIR,
  )

  logger.success("Audio processing complete")
