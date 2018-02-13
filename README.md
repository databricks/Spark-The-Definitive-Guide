# Spark: The Definitive Guide


This is the central repository for all materials related to [Spark: The Definitive Guide](http://shop.oreilly.com/product/0636920034957.do) by Bill Chambers and Matei Zaharia. 

*This repository is currently a work in progress and new material will be added over time.*

![Spark: The Definitive Guide](https://images-na.ssl-images-amazon.com/images/I/51z7TzI-Y3L._SX379_BO1,204,203,200_.jpg)

# Code from the Book

Code fom the book can be found in the "code" sub-folder where it is broken down by language and chapter. Pull requests are more than welcome for those that might want to provide a different layout.

# Instructions for how to run this code

## Running on your local machine

To run this on your local machine either put all data in the "data" folder to `/data` on your computer. Another option is that when reading in data from the book, simply specify the path to that particular dataset, on your local machine.

## Running on Databricks

The entire book was written in Databricks so all the code will run there just fine in Databricks Runtime 3.1 or later.

Rather than having to upload the data, you'll simply be able to use `"/databricks-datasets/"` as the base path for the data. *TODO: this is still be uploaded, expect it in the coming weeks*.

You can then run any of the notebooks in `code` folder inside of Databricks by simply importing them and running them.



