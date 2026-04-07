# Spark File Ingestion Toolkit - Task Runner

# Default PySpark/Python versions for Docker testing
PYSPARK_VERSION := env("PYSPARK_VERSION", "4.1")
DELTA_VERSION := env("DELTA_VERSION", "4.1")
PYTHON_VERSION := env("PYTHON_VERSION", "3.12")

# Show all available commands
default:
    @just --list

# Format code with ruff
format:
    uv run ruff format .
    uv run ruff check . --fix

# Run all tests with coverage report (excludes demos)
test:
    PYSPARK_VERSION={{PYSPARK_VERSION}} DELTA_VERSION={{DELTA_VERSION}} PYTHON_VERSION={{PYTHON_VERSION}} docker compose build
    PYSPARK_VERSION={{PYSPARK_VERSION}} DELTA_VERSION={{DELTA_VERSION}} PYTHON_VERSION={{PYTHON_VERSION}} docker compose up -d spark-test
    sleep 3
    docker compose exec spark-test pytest tests/ --ignore=tests/demo --cov
    docker compose down

# Run tests for all supported PySpark versions
test-all:
    PYSPARK_VERSION=3.5 DELTA_VERSION=3.3 PYTHON_VERSION=3.12 just test
    PYSPARK_VERSION=4.0 DELTA_VERSION=4.0 PYTHON_VERSION=3.13 just test
    PYSPARK_VERSION=4.1 DELTA_VERSION=4.1 PYTHON_VERSION=3.13 just test

# Run tests without Docker (requires local Spark)
test-local:
    pytest tests/ --cov

# Run specific test file
test-file file:
    docker compose exec spark-test pytest {{file}} -v

# Run demos with verbose output (shows what's happening)
demo:
    docker compose up -d spark-test
    @echo "Running demos with verbose output..."
    docker compose exec spark-test pytest tests/demo/ -v -s --tb=short
    docker compose down

# Build package
build:
    uv build

# Clean compiled files and caches
clean:
    find . -type f -name "*.py[co]" -delete
    find . -type d -name "__pycache__" -delete
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
    rm -rf .pytest_cache dist build .coverage htmlcov

# Convert CRLF to LF line endings
fix-line-endings:
    find . -type f \( -name "*.py" -o -name "*.md" -o -name "*.json" -o -name "*.yaml" -o -name "*.yml" -o -name "*.toml" -o -name "*.txt" -o -name "*.csv" -o -name "*.feature" \) -not -path "./.venv/*" -not -path "./.git/*" -exec sed -i 's/\r$//' {} +

# Start Docker Spark container
up:
    PYSPARK_VERSION={{PYSPARK_VERSION}} DELTA_VERSION={{DELTA_VERSION}} PYTHON_VERSION={{PYTHON_VERSION}} docker compose up -d spark-test
    @echo "Spark container started"

# Stop Docker Spark container
down:
    docker compose down

# Open test report in browser
report:
    #!/usr/bin/env bash
    if [ -f "report.html" ]; then
        if command -v explorer.exe &> /dev/null; then
            explorer.exe report.html
        elif command -v xdg-open &> /dev/null; then
            xdg-open report.html
        elif command -v open &> /dev/null; then
            open report.html
        else
            echo "Report: $(pwd)/report.html"
        fi
    else
        echo "No report.html found. Run 'just test' first."
    fi

# Run linting checks (no fixes)
lint:
    uv run ruff check .

# Type check with mypy
typecheck:
    uv run mypy file_processing/ dataframe_validation/ table_writing/ file_management/

# Run all quality checks
check: format lint typecheck test

# Install development dependencies
install:
    uv sync --all-extras
    uv run pre-commit install

# Update dependencies
update:
    uv lock --upgrade

# Show project info
info:
    @echo "Spark File Ingestion Toolkit"
    @echo "Python: $(python --version)"
    @echo "uv: $(uv --version)"
    @echo "Docker: $(docker --version)"
