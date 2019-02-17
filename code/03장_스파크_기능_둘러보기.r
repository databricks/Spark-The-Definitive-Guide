library(SparkR)
sparkDF <- read.df("/data/flight-data/csv/2015-summary.csv",
         source = "csv", header="true", inferSchema = "true")
take(sparkDF, 5)


# COMMAND ----------

collect(orderBy(sparkDF, "count"), 20)


# COMMAND ----------

library(magrittr)
sparkDF %>%
  orderBy(desc(sparkDF$count)) %>%
  groupBy("ORIGIN_COUNTRY_NAME") %>%
  count() %>%
  limit(10) %>%
  collect()


# COMMAND ----------

