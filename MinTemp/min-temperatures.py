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


def parse_line(line: str) -> Optional[Tuple[str, str, float]]:
    """
    Parses a line of the CSV file and extracts the station ID, entry type, and temperature.

    Args:
    line (str): A line of text from the CSV file.

    Returns:
    Optional[Tuple[str, str, float]]: A tuple containing the station ID, entry type, and
    temperature in Fahrenheit, or None if there is an error.
    """
    try:
        fields = line.split(",")
        station_id = fields[0]
        entry_type = fields[2]
        temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
        return station_id, entry_type, temperature
    except (IndexError, ValueError) as e:
        logging.error(f"Error parsing line: {line}. Error: {e}")
        return None


def main() -> None:
    """
    Main function to configure Spark, process the temperature dataset,
    and print the minimum temperatures by station.
    """
    configure_logging()

    try:
        # Configure Spark
        conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
        sc = SparkContext(conf=conf)
        logging.info("Spark context initialized.")
    except Exception as e:
        logging.error(f"Error initializing Spark context: {e}")
        return

    try:
        # Load the temperature dataset
        lines = sc.textFile("file:///SparkCourse/MinTemp/1800.csv")
        logging.info("Dataset loaded.")
    except Exception as e:
        logging.error(f"Error loading dataset: {e}")
        return

    try:
        # Parse each line to extract station ID, entry type, and temperature
        parsed_lines = lines.map(parse_line).filter(lambda x: x is not None)

        # Filter out all entries except for TMIN entries
        min_temps = parsed_lines.filter(lambda x: "TMIN" in x[1])

        # Create (stationID, temperature) tuples
        station_temps = min_temps.map(lambda x: (x[0], x[2]))

        # Reduce by stationID retaining the minimum temperature found
        min_temps_by_station = station_temps.reduceByKey(lambda x, y: min(x, y))

        # Collect the results
        results: List[Tuple[str, float]] = min_temps_by_station.collect()

        # Print the results
        for result in results:
            logging.info(f"{result[0]}\t{result[1]:.2f}F")
    except Exception as e:
        logging.error(f"Error processing dataset: {e}")
    finally:
        sc.stop()
        logging.info("Spark context stopped.")


if __name__ == "__main__":
    main()
