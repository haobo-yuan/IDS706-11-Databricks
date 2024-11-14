# Makefile

.PHONY: install test format lint all

install:
	pip install --upgrade pip && \
		pip install -r requirements.txt

test:
	# Because this needs to be run on DataBricks, we can't run this locally/GitHub Actions
	# pytest -vv --cov=lib --cov-report=term-missing test_*.py
	# pytest -vv --cov=lib --cov-report=term-missing --nbval *.ipynb test_*.py
	
format:
	black *.py
	nbqa black *.ipynb

lint:
	# Because this needs to be run on DataBricks, we can't run this locally/GitHub Actions
	# ruff check *.py
	# nbqa ruff *.ipynb

all: install format lint test
