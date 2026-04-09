"""
=============================================================================
config_loader.py — Configuration and Environment Variable Loader
=============================================================================
Reads config.yaml and substitutes ${ENV_VAR} placeholders with actual
values from the environment (loaded from a .env file first).
=============================================================================
"""

import os
import re
import yaml
from dotenv import load_dotenv
from typing import Any, Dict

# Load .env file into the environment before anything else
load_dotenv()


def _substitute_env_vars(value: Any) -> Any:
    """
    Recursively walk through the parsed YAML structure and replace
    ${VAR_NAME} placeholders with actual environment variable values.

    Args:
        value: A YAML value — could be a dict, list, or scalar.

    Returns:
        The value with all placeholders resolved.

    Raises:
        EnvironmentError: If a required environment variable is not set.
    """
    if isinstance(value, dict):
        return {k: _substitute_env_vars(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_substitute_env_vars(item) for item in value]

    if isinstance(value, str):
        # Match patterns like ${VAR_NAME}
        pattern = re.compile(r"\$\{([^}]+)\}")
        matches = pattern.findall(value)
        for var_name in matches:
            env_val = os.getenv(var_name)
            if env_val is None:
                raise EnvironmentError(
                    f"Required environment variable '{var_name}' is not set. "
                    f"Please define it in your .env file or system environment."
                )
            value = value.replace(f"${{{var_name}}}", env_val)
        return value

    return value  # int, float, bool, None — return as-is


def load_config(config_path: str = "config/config.yaml") -> Dict[str, Any]:
    """
    Load and return the pipeline configuration dictionary.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        dict: Fully resolved configuration dictionary.

    Raises:
        FileNotFoundError : If the config file does not exist.
        EnvironmentError  : If any required env variable is missing.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found at '{config_path}'. "
            f"Ensure config/config.yaml exists in the project root."
        )

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    # Resolve all ${ENV_VAR} placeholders
    resolved_config = _substitute_env_vars(raw_config)
    return resolved_config
