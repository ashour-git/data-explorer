# Data Archaeologist Framework - Professional Makefile
# Enterprise-grade database discovery and analysis framework

.PHONY: help install install-dev test lint format clean docs build

# Default target
help:
	@echo "Data Archaeologist Framework - Available Commands:"
	@echo ""
	@echo "  install       Install the framework in production mode"
	@echo "  install-dev   Install the framework in development mode"
	@echo "  interactive   Launch interactive workflow terminal"
	@echo "  test          Run the test suite"
	@echo "  lint          Run code linting"
	@echo "  format        Format code using black and isort"
	@echo "  clean         Clean build artifacts and cache files"
	@echo "  docs          Generate documentation"
	@echo "  build         Build distribution packages"
	@echo "  run-discovery Run complete database discovery"
	@echo "  summarize     Run database summary analysis"
	@echo "  test-staging  Test staging database connection"
	@echo "  test-prod     Test production database connection"
	@echo "  test-backup   Test backup database connection"
	@echo ""

# Installation targets
install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pip install pytest black isort flake8 mypy

# Interactive workflow
interactive:
	python scripts/interactive_workflow.py

# Development targets
test:
	pytest tests/ -v --cov=data_archaeologist

lint:
	flake8 data_archaeologist/ scripts/
	mypy data_archaeologist/

format:
	black data_archaeologist/ scripts/
	isort data_archaeologist/ scripts/

# Cleanup targets
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .coverage
	rm -rf .pytest_cache/
	rm -rf logs/

# Documentation targets
docs:
	@echo "Generating documentation..."
	@echo "Framework documentation available in README.md"

# Build targets
build: clean
	python setup.py sdist bdist_wheel

# Analysis targets
run-discovery:
	python -m data_archaeologist.archaeologist --environment staging

summarize:
	python scripts/database_summary_real.py --environment staging --format console

# Database connection testing
test-staging:
	python scripts/database_summary_real.py --environment staging --test-connection

test-prod:
	python scripts/database_summary_real.py --environment production --test-connection

test-backup:
	python scripts/database_summary_real.py --environment backup --test-connection

test-connections:
	python -c "from data_archaeologist.core import DatabaseConnection; db = DatabaseConnection(); print('Testing connections...'); [print(f'{env}: {db.test_connection(env)}') for env in db.get_available_environments()]"

# Professional deployment targets
deploy-staging:
	@echo "Deploying to staging environment..."
	# Add deployment commands here

deploy-production:
	@echo "Deploying to production environment..."
	# Add production deployment commands here
