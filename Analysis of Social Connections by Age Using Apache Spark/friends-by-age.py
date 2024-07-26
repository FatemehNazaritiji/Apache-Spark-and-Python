from pyspark import SparkConf, SparkContext
from typing import Tuple, List


def parseLine(line: str) -> Tuple[int, int]:
    """
    Parses a line of the CSV file and extracts the age and number of friends.

    Args:
    line (str): A line of text from the CSV file.

    Returns:
    Tuple[int, int]: A tuple containing the age and number of friends.
    """
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


def main() -> None:
    """
    Main function to set up Spark, process the data, and print the average number of friends by age.
    """
    # Configure Spark
    conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
    sc = SparkContext(conf=conf)

    # Read the CSV file
    lines = sc.textFile("file:///SparkCourse/friend-by-age/fakefriends.csv")

    # Parse each line to extract age and number of friends
    rdd = lines.map(parseLine)

    # Map values to (numFriends, 1) for each age and then reduce by key to sum the
    # number of friends and count occurrences
    totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(
        lambda x, y: (x[0] + y[0], x[1] + y[1])
    )

    # Calculate the average number of friends by age
    averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

    # Collect the results to the driver
    results: List[Tuple[int, float]] = averagesByAge.collect()

    # Print the results
    for result in results:
        print(result)

    # Stop the SparkContext
    sc.stop()


if __name__ == "__main__":
    main()
