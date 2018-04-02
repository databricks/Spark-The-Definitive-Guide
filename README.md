# Spark: The Definitive Guide

This is the central repository for all materials related to [Spark: The Definitive Guide](http://shop.oreilly.com/product/0636920034957.do) by Bill Chambers and Matei Zaharia. 

*This repository is currently a work in progress and new material will be added over time.*

![Spark: The Definitive Guide](https://images-na.ssl-images-amazon.com/images/I/51z7TzI-Y3L._SX379_BO1,204,203,200_.jpg)

# Code from the book

You can find the code from the book in the `code` subfolder where it is broken down by language and chapter.

# How to run the code

## Run on your local machine

To run the example on your local machine, either pull all data in the `data` subfolder to `/data` on your computer or specify the path to that particular dataset on your local machine.

## Run on Databricks

Databricks is a zero-management cloud platform that provides:

- Fully managed Spark clusters
- An interactive workspace for exploration and visualization
- A production pipeline scheduler
- A platform for powering your favorite Spark-based applications

All the examples run on Databricks Runtime 3.1 and above. To get a free Databricks account, go to [Try Databricks](https://databricks.com/try-databricks).

On a Databricks cluster, the data required to run the examples is available in `/databricks-datasets/definitive-guide/data`.

You can run a notebook in the `code` folder in Databricks after you import it, update the path to the data file to `/databricks-datasets/definitive-guide/data`, and attach it to a cluster. For details, see [Notebooks](https://docs.databricks.com/user-guide/notebooks/index.html).
