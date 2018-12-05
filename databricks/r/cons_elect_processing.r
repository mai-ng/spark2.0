# Databricks notebook source
library(SparkR)

# COMMAND ----------

customSchema <- structType(
  structField("jour", type = "timestamp"),
  structField("cate_client", type = "string"),
  structField("cons_elect", type = "double")
)

# COMMAND ----------

getDF <- function(filePath, argschema, argsource = "csv", argheader = "true", argsep = ";"){
  if(is.null(argschema)){
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, inferSchema = "true")
  }else{
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, schema = argschema)
  }
  return(df)
}

# COMMAND ----------

cons_electDF <- getDF("FileStore/tables/test/cons_elec.csv", customSchema)
head(cons_electDF)

# COMMAND ----------

mutate(cons_electDF, cons_electDF$an <- year(cons_electDF$jour))
head(cons_electDF)

# COMMAND ----------

groupedByYear <- agg(groupBy(cons_electDF, "an", "cate_client"), cons_sum = sum(cons_electDF$cons_elect))
head(groupedByYear)

# COMMAND ----------

sortedGroupedByYear <- arrange(groupedByYear, groupedByYear$an)
head(sortedGroupedByYear)

# COMMAND ----------

groupedByCateClient <- agg(groupBy( cons_electDF, "cate_client"), cons_sum = sum(cons_electDF$cons_elect))
collect(groupedByCateClient)

# COMMAND ----------

groupByCateClient2017 <- agg(groupBy( filter(cons_electDF, cons_electDF$an == 2017), cons_electDF$cate_client, cons_electDF$an), cons_sum = sum(cons_electDF$cons_elect))
collect(groupByCateClient2017)
