import yaml
import os
from typing import Dict, Any


class Config:
    """
    Configuration manager for SparkCity project

    Usage:
        >>> from src.utils.config_loader import Config
        >>> config = Config()
        >>> spark_url = config.get('spark', 'master_url')
        >>> db_host = config.get('postgres', 'host')
    """

    def __init__(self, config_path=None):
        """
        Initialize config loader

        Args:
            config_path (str): Optional path to config file
        """
        if config_path is None:
            # Default to team_config.yaml
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            config_path = os.path.join(base_dir, "config", "team_config.yaml")

        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load YAML config file"""
        try:
            with open(self.config_path, "r") as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in config file: {e}")

    def get(self, *keys, default=None):
        """
        Get nested configuration value

        Args:
            *keys: Nested keys to traverse
            default: Default value if key not found

        Returns:
            Configuration value or default

        Example:
            >>> config.get('spark', 'master_url')
            'spark://localhost:7077'
        """
        value = self.config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def get_spark_config(self) -> Dict[str, Any]:
        """Get all Spark configuration"""
        return self.config.get("spark", {})

    def get_postgres_config(self) -> Dict[str, Any]:
        """Get all PostgreSQL configuration"""
        return self.config.get("postgres", {})

    def get_data_paths(self) -> Dict[str, str]:
        """Get all data path configuration"""
        return self.config.get("data_paths", {})


# Example usage
if __name__ == "__main__":
    print("Testing config loader...")
    config = Config()
    print(f"✅ Spark master: {config.get('spark', 'master_url')}")
    print(f"✅ DB host: {config.get('postgres', 'host')}")
    print(f"✅ Raw data path: {config.get('data_paths', 'raw')}")
    print("✅ Config loader working!")
