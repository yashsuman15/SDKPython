# Project Structure

- `labellerr_sdk/`
- `labellerr/`
  - `client.py` - Main client class for interacting with the APIs
  - `api_handler.py` - Handles HTTP requests (GET, POST, etc.)
  - `exceptions.py` - Custom exception handling
  - `utils.py` - Utility functions (e.g., UUID generation, validation)
  - `config.py` - Configuration (like base URLs)
- `tests/`
  - `test_client.py` - Unit tests for the client
- `README.md` - Documentation on how to install and use the SDK
- `setup.py` - Package setup script for pip installation
- `pyproject.toml` - Dependency and build configuration
- `LICENSE` - License information

# Labellerr Client

A Python client for interacting with the Labellerr API.

# Installation

You can install the package using pip:
- !pip install https://github.com/tensormatics/SDKPython/releases/download/v1/labellerr_sdk-0.1.0.tar.gz
