import argparse
import sys
from datetime import datetime
from src.utils.spark_session import get_spark_session, stop_spark_session
from src.data_ingestion.load_all import load_all_datasets
from src.database.db_writer import DatabaseWriter
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def run_pipeline(
    data_dir: str = "data/samples", mode: str = "append", validate: bool = True
):
    """
    Run the complete SparkCity ETL pipeline

    Args:
        data_dir: Directory containing data files
        mode: Write mode ('append' or 'overwrite')
        validate: Whether to validate data quality

    Returns:
        bool: True if successful, False otherwise
    """
    spark = None
    start_time = datetime.now()

    logger.info("SPARKCITY ETL PIPELINE START")
    logger.info("=" * 80)
    logger.info(f"Start Time: {start_time}")
    logger.info(f"Data Directory: {data_dir}")
    logger.info(f"Write Mode: {mode}")
    logger.info(f"Validation: {'Enabled' if validate else 'Disabled'}")

    try:
        # STEP 1: Initialize Spark
        logger.info("\nSTEP 1: Initializing Spark Session...")
        spark = get_spark_session("SparkCityPipeline")
        logger.info("Spark session created successfully")

        # STEP 2: Data Ingestion
        logger.info("\nSTEP 2: Loading Datasets...")
        datasets = load_all_datasets(spark, data_dir=data_dir, validate=validate)
        logger.info("All datasets loaded successfully")

        # Log dataset sizes
        logger.info("\nDataset Summary:")
        logger.info(f"  • Traffic:     {datasets['traffic'].count():,} records")
        logger.info(f"  • Air Quality: {datasets['air_quality'].count():,} records")
        logger.info(f"  • Weather:     {datasets['weather'].count():,} records")
        logger.info(f"  • Energy:      {datasets['energy'].count():,} records")
        logger.info(f"  • Zones:       {datasets['city_zones'].count():,} zones")

        total_records = (
            datasets["traffic"].count()
            + datasets["air_quality"].count()
            + datasets["weather"].count()
            + datasets["energy"].count()
        )
        logger.info(f"  • TOTAL:       {total_records:,} records")

        # STEP 3: Data Quality (placeholder for team to expand)
        logger.info("\n STEP 3: Data Quality Checks...")
        logger.info(" Using validation from data ingestion")
        logger.info("   (Team can add advanced cleaning here)")

        # STEP 4: Write to Database
        logger.info("\n STEP 4: Writing to PostgreSQL...")
        db_writer = DatabaseWriter(spark)
        db_writer.write_all_data(datasets, mode=mode)
        logger.info("All data written to database")

        # STEP 5: Pipeline Complete
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logger.info("SPARKCITY ETL PIPELINE COMPLETE")
        logger.info(f"End Time: {end_time}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Records Processed: {total_records:,}")
        logger.info(f"Records/Second: {total_records / duration:.2f}")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error("PIPELINE FAILED")
        logger.error(f"Error: {e}")
        import traceback

        traceback.print_exc()
        logger.error("=" * 80)
        return False

    finally:
        if spark:
            stop_spark_session(spark)
            logger.info("\nSpark session stopped")


def main():
    """
    Command-line interface for pipeline
    """
    parser = argparse.ArgumentParser(
        description="SparkCity ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with sample data
  python src/pipeline/main.py

  # Run with custom data directory
  python src/pipeline/main.py --data-dir data/raw

  # Overwrite existing data
  python src/pipeline/main.py --mode overwrite

  # Skip validation for faster processing
  python src/pipeline/main.py --no-validate
        """,
    )

    parser.add_argument(
        "--data-dir",
        type=str,
        default="data/samples",
        help="Directory containing data files (default: data/samples)",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["append", "overwrite"],
        default="append",
        help="Database write mode (default: append)",
    )

    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip data validation for faster processing",
    )

    args = parser.parse_args()

    # Run pipeline
    success = run_pipeline(
        data_dir=args.data_dir, mode=args.mode, validate=not args.no_validate
    )

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
