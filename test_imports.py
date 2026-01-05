"""
Quick test to verify team utilities can be imported
"""

print("Testing imports...")

try:
    from src.utils.spark_session import get_spark_session, stop_spark_session

    print("✅ spark_session imports work")
except ImportError as e:
    print(f"❌ spark_session import failed: {e}")

try:
    from src.utils.config_loader import Config

    print("✅ config_loader imports work")
except ImportError as e:
    print(f"❌ config_loader import failed: {e}")

try:
    from src.utils.logger import setup_logger

    print("✅ logger imports work")
except ImportError as e:
    print(f"❌ logger import failed: {e}")

print("\n✅ All utilities can be imported!")
