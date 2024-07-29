import logging
from pyspark import SparkConf, SparkContext
import collections
from typing import Dict, OrderedDict, Optional


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def parse_rating(line: str) -> Optional[str]:
    """
    Extracts the rating from a line of the dataset.

    Args:
    line (str): A line of text from the dataset.

    Returns:
    Optional[str]: The extracted rating, or None if there is an error.
    """
    try:
        rating = line.split()[2]
        if rating.isdigit():
            return rating
        else:
            logging.error(f"Non-numeric rating encountered: {rating}")
            return None
    except IndexError as e:
        logging.error(f"Error parsing line: {line}. Error: {e}")
        return None


def main() -> None:
    """
    Main function to configure Spark, process the MovieLens 100K dataset,
    and print the histogram of movie ratings.
    """
    configure_logging()

    try:
        # Configure Spark
        conf: SparkConf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
        sc: SparkContext = SparkContext(conf=conf)
        logging.info("Spark context initialized.")
    except Exception as e:
        logging.error(f"Error initializing Spark context: {e}")
        return

    try:
        # Load the MovieLens 100K dataset
        lines = sc.textFile(
            "file:///SparkCourse/Movie Ratings Histogram using Apache Spark/ml-100k/u.data"
        )
        logging.info("Dataset loaded.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return

    try:
        # Extract ratings from the dataset
        ratings = lines.map(parse_rating).filter(lambda x: x is not None)

        # Count the occurrence of each rating
        result: Dict[str, int] = ratings.countByValue()

        # Sort the results
        sorted_results: OrderedDict[str, int] = collections.OrderedDict(
            sorted(result.items())
        )

        # Print the sorted results
        for key, value in sorted_results.items():
            logging.info(f"{key} {value}")
    except Exception as e:
        logging.error(f"Error processing dataset: {e}")
    finally:
        sc.stop()
        logging.info("Spark context stopped.")


if __name__ == "__main__":
    main()
