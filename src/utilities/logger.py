import logging
import sys
from datetime import datetime


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Create a configured logger

    Args:
        name (str): Logger name (usually __name__)
        level: Logging level (default: INFO)

    Returns:
        logging.Logger: Configured logger

    Example:
        >>> from src.utils.logger import setup_logger
        >>> logger = setup_logger(__name__)
        >>> logger.info("Processing started")
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding handlers multiple times
    if not logger.handlers:
        # Console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        # Format: timestamp - name - level - message
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


# Example usage
if __name__ == "__main__":
    logger = setup_logger(__name__)
    logger.info("✅ Logger initialized")
    logger.warning("⚠️ This is a warning")
    logger.error("❌ This is an error")
    print("✅ Logger working!")
