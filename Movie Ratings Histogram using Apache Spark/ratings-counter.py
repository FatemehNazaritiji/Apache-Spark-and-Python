from pyspark import SparkConf, SparkContext
import collections
from typing import Dict, OrderedDict


def main() -> None:
    """
    Main function to configure Spark, process the MovieLens 100K dataset,
    and print the histogram of movie ratings.
    """
    # Configure Spark
    conf: SparkConf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
    sc: SparkContext = SparkContext(conf=conf)

    # Load the MovieLens 100K dataset
    lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

    # Extract ratings from the dataset
    ratings = lines.map(lambda x: x.split()[2])

    # Count the occurrence of each rating
    result: Dict[str, int] = ratings.countByValue()

    # Sort the results
    sorted_results: OrderedDict[str, int] = collections.OrderedDict(
        sorted(result.items())
    )

    # Print the sorted results
    for key, value in sorted_results.items():
        print("%s %i" % (key, value))


if __name__ == "__main__":
    main()
