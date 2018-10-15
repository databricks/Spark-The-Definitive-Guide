library(SparkR)
spark <- sparkR.session()


# COMMAND ----------

retail.data <- read.df(
  "/data/retail-data/all/",
  "csv",
  header="true",
  inferSchema="true")
print(str(retail.data))


# COMMAND ----------

local.retail.data <- take(retail.data, 5)
print(str(local.retail.data))


# COMMAND ----------

# collect brings it from Spark to your local environment
collect(count(groupBy(retail.data, "country")))
# createDataFrame comverts a data.frame
# from your local environment to Spark


# COMMAND ----------

?na.omit # refers to SparkR due to package loading order
?stats::na.omit # refers explicitly to stats
?SparkR::na.omit # refers explicitly to sparkR's null value filtering


# COMMAND ----------

sample(mtcars) # fails


# COMMAND ----------

base::sample(some.r.data.frame) # some.r.data.frame = R data.frame type


# COMMAND ----------

?to_date # to Data DataFrame column manipulation


# COMMAND ----------

tbls <- sql("SHOW TABLES")

collect(
  select(
    filter(tbls, like(tbls$tableName, "%production%")),
    "tableName",
    "isTemporary"))


# COMMAND ----------

library(magrittr)

tbls %>%
  filter(like(tbls$tableName, "%production%")) %>%
  select("tableName", "isTemporary") %>%
  collect()


# COMMAND ----------

retail.data <- read.df(
  "/data/retail-data/all/",
  "csv",
  header="true",
  inferSchema="true")
flight.data <- read.df(
  "/data/flight-data/parquet/2010-summary.parquet",
  "parquet")


# COMMAND ----------

model <- spark.glm(retail.data, Quantity ~ UnitPrice + Country,
  family='gaussian')
summary(model)
predict(model, retail.data)

write.ml(model, "/tmp/myModelOutput", overwrite=T)
newModel <- read.ml("/tmp/myModelOutput")


# COMMAND ----------

families <- c("gaussian", "poisson")
train <- function(family) {
  model <- glm(Sepal.Length ~ Sepal.Width + Species, iris, family = family)
  summary(model)
}
# Return a list of model's summaries
model.summaries <- spark.lapply(families, train)

# Print the summary of each model
print(model.summaries)


# COMMAND ----------

df <- withColumnRenamed(createDataFrame(as.data.frame(1:100)), "1:100", "col")
outputSchema <- structType(
  structField("col", "integer"),
  structField("newColumn", "double"))

udfFunc <- function (remote.data.frame) {
  remote.data.frame['newColumn'] = remote.data.frame$col * 2
  remote.data.frame
}
# outputs SparkDataFrame, so it requires a schema
take(dapply(df, udfFunc, outputSchema), 5)
# collects all results to a, so no schema required.
# however this will fail if the result is large
dapplyCollect(df, udfFunc)


# COMMAND ----------

local <- as.data.frame(1:100)
local['groups'] <- c("a", "b")

df <- withColumnRenamed(createDataFrame(local), "1:100", "col")

outputSchema <- structType(
  structField("col", "integer"),
  structField("groups", "string"),
  structField("newColumn", "double"))

udfFunc <- function (key, remote.data.frame) {
  if (key == "a") {
    remote.data.frame['newColumn'] = remote.data.frame$col * 2
  } else if (key == "b") {
    remote.data.frame['newColumn'] = remote.data.frame$col * 3
  } else if (key == "c") {
    remote.data.frame['newColumn'] = remote.data.frame$col * 4
  }

  remote.data.frame
}
# outputs SparkDataFrame, so it requires a schema
take(gapply(df,
            "groups",
            udfFunc,
            outputSchema), 50)

gapplyCollect(df,
             "groups",
             udfFunc)


# COMMAND ----------

install.packages("sparklyr")
library(sparklyr)


# COMMAND ----------

sc <- spark_connect(master = "local")


# COMMAND ----------

spark_connect(master = "local", config = spark_config())


# COMMAND ----------

library(DBI)
allTables <- dbGetQuery(sc, "SHOW TABLES")


# COMMAND ----------

setShufflePartitions <- dbGetQuery(sc, "SET spark.sql.shuffle.partitions=10")


# COMMAND ----------

spark_write_csv(tbl_name, location)
spark_write_json(tbl_name, location)
spark_write_parquet(tbl_name, location)


# COMMAND ----------

