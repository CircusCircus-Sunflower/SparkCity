from src.utils.spark_session import get_spark_session, stop_spark_session
from src.data_ingestion.load_all import load_all_datasets
from src.database.db_writer import DatabaseWriter
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def main():
    """
    Load sample data from files into PostgreSQL database
    """
    logger.info("=" * 60)
    logger.info("SparkCity ETL Pipeline - Sample Data Load")
    logger.info("=" * 60)

    spark = None

    try:
        # Step 1: Create Spark session
        logger.info("\nStep 1: Connecting to Spark cluster...")
        spark = get_spark_session("SampleDataLoad")

        # Step 2: Load data from files
        logger.info("\nStep 2: Loading sample datasets...")
        datasets = load_all_datasets(spark, data_dir="data/samples", validate=True)

        # Step 3: Initialize database writer
        logger.info("\nStep 3: Initializing database writer...")
        db_writer = DatabaseWriter(spark)

        # Step 4: Write all data to database
        logger.info("\nStep 4: Writing data to PostgreSQL...")
        db_writer.write_all_data(datasets, mode="append")

        logger.info("\n" + "=" * 60)
        logger.info("ETL Pipeline Complete!")
        logger.info("=" * 60)
        logger.info("\nData has been loaded into PostgreSQL.")
        logger.info("You can now query the database or view in dashboard.")
        logger.info("\nTo verify, run:")
        logger.info(
            "  docker exec -it postgres-smartcity psql -U postgres -d smartcity"
        )
        logger.info("  SELECT COUNT(*) FROM fact_traffic;")

    except Exception as e:
        logger.error(f"\nETL Pipeline failed: {e}")
        import traceback

        traceback.print_exc()

    finally:
        if spark:
            stop_spark_session(spark)
            logger.info("\nSpark session stopped")


if __name__ == "__main__":
    main()
