import logging
from pyspark import SparkConf, SparkContext
from typing import Tuple, List


def configure_logging() -> None:
    """
    Configures the logging settings for the script.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def extract_customer_price_pairs(line: str) -> Tuple[int, float]:
    """
    Parses a line of the CSV file and extracts the customer ID and price.

    Args:
    line (str): A line of text from the CSV file.

    Returns:
    Tuple[int, float]: A tuple containing the customer ID and price.
    """
    try:
        fields = line.split(",")
        customer_id = int(fields[0])
        price = float(fields[2])
        return customer_id, price
    except (IndexError, ValueError) as e:
        logging.error(f"Error parsing line: {line}. Error: {e}")
        return None, 0.0


def main() -> None:
    """
    Main function to configure Spark, process the customer orders dataset,
    and print the total amount spent by each customer, sorted by total spend.
    """
    configure_logging()

    try:
        # Configure Spark
        conf: SparkConf = (
            SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
        )
        sc: SparkContext = SparkContext(conf=conf)
        logging.info("Spark context initialized.")
    except Exception as e:
        logging.error(f"Error initializing Spark context: {e}")
        return

    try:
        # Load the customer orders dataset
        input_rdd = sc.textFile(
            "file:///sparkcourse/Total Amount Spent by Customer/customer-orders.csv"
        )
        logging.info("Dataset loaded.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return

    try:
        # Parse each line to extract customer ID and price
        mapped_input = input_rdd.map(extract_customer_price_pairs)

        # Reduce by customer ID to get the total amount spent by each customer
        total_by_customer = mapped_input.reduceByKey(lambda x, y: x + y)

        # Flip the (customerID, total) to (total, customerID) for sorting
        flipped = total_by_customer.map(lambda x: (x[1], x[0]))

        # Sort by total spend
        total_by_customer_sorted = flipped.sortByKey()

        # Collect the results
        results: List[Tuple[float, int]] = total_by_customer_sorted.collect()

        # Print the results
        for total, customer_id in results:
            logging.info(f"Customer ID: {customer_id}, Total Spent: {total:.2f}")
    except Exception as e:
        logging.error(f"Error processing dataset: {e}")
    finally:
        sc.stop()
        logging.info("Spark context stopped.")


if __name__ == "__main__":
    main()
