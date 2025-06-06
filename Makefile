# Makefile for Python project

.DEFAULT_GOAL := help
.PHONY := help venv install clean update_requirement_file

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
CRAWLER := tts_data_pipeline/crawler/main.py
PRE_PROCESSING := tts_data_pipeline/pre_processing/main.py
ALIGNMENT := tts_data_pipeline/alignment/main.py

help: 
	@echo "Available targets:"
	@echo "  venv: Create virtual environment"
	@echo "  install: Install dependencies"
	@echo "  clean: Clean virtual environment"
	@echo "  freeze: Update requirements.txt file"
	@echo "  download_book_all: Download all books"
	@echo "  fetch_metadata: Fetch metadata for each book"
	@echo "  create_metadata_csv: Create metadata CSV file"

venv:
	@echo "Creating virtual environment..."
	@python3 -m venv .venv
	@echo "Done"

install: venv
	@echo "Installing dependencies..."
	@$(PIP) install -r requirements.txt
	@echo "Done"

clean:
	@echo "Cleaning virtual environment..."
	@rm -rf .venv
	@echo "Done"

freeze:
	@echo "Updating requirements.txt file..."
	@$(PYTHON) -m pip freeze > requirements.txt
	@echo "Done"

download_book_all:
	@echo "Downloading all books..."
	@$(PYTHON) $(CRAWLER) --download "all"
	@echo "Done"

fetch_metadata:
	@echo "Fetching metadata for each book..."
	@$(PYTHON) $(CRAWLER) --fetch-metadata
	@echo "Done"

create_metadata_csv:
	@echo "Create metadata CSV file..."
	@$(PYTHON) $(CRAWLER) --create-metadata-csv
	@echo "Done"

pre_processing_all:
	@echo "Pre-processing all books..."
	@$(PYTHON) $(PRE_PROCESSING) -t "all"
	@echo "Done"
