import logging
from pyspark import SparkConf, SparkContext
from typing import Tuple, List, Optional


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def parse_line(line: str) -> Optional[Tuple[int, int]]:
    """
    Parses a line of the CSV file and extracts the age and number of friends.

    Args:
    line (str): A line of text from the CSV file.

    Returns:
    Optional[Tuple[int, int]]: A tuple containing the age and number of friends, or None if there is an error.
    """
    try:
        fields = line.split(",")
        age = int(fields[2])
        num_friends = int(fields[3])
        return age, num_friends
    except (IndexError, ValueError) as e:
        logging.error(f"Error parsing line: {line}. Error: {e}")
        return None


def main() -> None:
    """
    Main function to set up Spark, process the data, and print the average number of friends by age.
    """
    configure_logging()

    try:
        # Configure Spark
        conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
        sc = SparkContext(conf=conf)
        logging.info("Spark context initialized.")
    except Exception as e:
        logging.error(f"Error initializing Spark context: {e}")
        return

    try:
        # Read the CSV file
        lines = sc.textFile("file:///SparkCourse/friend-by-age/fakefriends.csv")
        logging.info("Dataset loaded.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return

    try:
        # Parse each line to extract age and number of friends
        rdd = lines.map(parse_line).filter(lambda x: x is not None)

        # Map values to (num_friends, 1) for each age and then reduce by key to sum the
        # number of friends and count occurrences
        totals_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
            lambda x, y: (x[0] + y[0], x[1] + y[1])
        )

        # Calculate the average number of friends by age
        averages_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1])

        # Collect the results to the driver
        results: List[Tuple[int, float]] = averages_by_age.collect()

        # Print the results
        for result in results:
            logging.info(f"Age: {result[0]}, Average Number of Friends: {result[1]}")
    except Exception as e:
        logging.error(f"Error processing dataset: {e}")
    finally:
        sc.stop()
        logging.info("Spark context stopped.")


if __name__ == "__main__":
    main()
