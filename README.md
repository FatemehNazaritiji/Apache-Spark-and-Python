# Data Analysis Projects Using Apache Spark

### Installation
To run this project, you need to have the following installed:

- **Apache Spark:** [Download Spark](https://spark.apache.org/downloads.html)
- **JDK 17:** [Download JDK 17](https://www.oracle.com/java/technologies/downloads/?er=221886)

## Project 1: Movie Ratings Histogram using Apache Spark

### Project Description
This project involves creating a histogram of movie ratings using the Apache Spark framework. The dataset used is the MovieLens 100K dataset, which contains 100,000 ratings from 1,000 users on 1,700 movies.

### Dataset
- **Datasets:** [MovieLens Datasets](https://grouplens.org/datasets/movielens/)
- **MovieLens 100K Dataset:** [Download Link](https://files.grouplens.org/datasets/movielens/ml-100k.zip)

### Running the Script
To execute the script, use the following command (I used Anaconda Prompt):

```sh
spark-submit ratings-counter.py
```



## Project 2: Analysis of Social Connections by Age Using Apache Spark

### Project Description
This project analyzes the number of friends by age using the Apache Spark framework. The analysis is based on a dataset that provides information about individuals and their number of friends.

### Dataset
The data used in this project is a synthetic dataset provided in a CSV file. Each row contains information about an individual, including their age and number of friends.

### Running the Script
To execute the script, use the following command (I used Anaconda Prompt):

```sh
spark-submit friends-by-age.py
```

### Reference
[Taming Big Data with Apache Spark - Hands On!](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on/?couponCode=ST3MT72524)
