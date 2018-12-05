############################################################################
## SparkR: Process data in a Spark DataFrame ##
## Maing, 12-2018 ##
############################################################################
## Objective: Read data about the electric consumation from a CSV file,
## then create a Spark DataFrame from the file.
## and process the data.

## * Initiate SparkR
## * Set up command line arguments (a script and an input file)
## * Create a function which creates a Spark DataFrame from a file: function()
## * Create a Spark DataFrame from an input files
## * Extract the column jour: select()
## * Add a new column which extracts year from the column jour: mutate()
## * Group by year and by category of client: agg(), groupBy(), sum()
## * Sort the grouped by year DF in an ascending order of year: arrange()
## * Group by the category of client: agg(), groupBy(), sum()
## * Sum of consommation of electricity for each category of client in 2017: filter(), agg(), groupBy(), sum()

## How to run the script: "spark-submit script_path input_path"
## e.g. spark-submit sparkr/cons_elect_processing.R /data/cons_elec.csv

# Set up command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Not enough arguments, Please specify the script following by the input file.\n", call.=FALSE)
} else {
  inputFile = args[1] # e.g. "/Users/nguyen/maing/training/r/data/cons_elec.csv"
}

# Load libraries
library(SparkR)

# Initiate a Spark Session
sparkR.session()

# Define a schema for the Spark DataFrame
customSchema <- structType(
  structField("jour", type = "timestamp"),
  structField("cate_client", type = "string"),
  structField("cons_elect", type = "double")
)

# Get a Spark Dataframe from a file
getDF <- function(filePath, argschema, argsource = "csv", argheader = "true", argsep = ";"){
  if(is.null(argschema)){
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, inferSchema = "true")
  }else{
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, schema = argschema)
  }
  return(df)
}

# Create a Spark DataFrame from a file
print("-----Create a Spark dataFrame")
cons_elec_df <- getDF(inputFile, customSchema)

# Head ans schema of the Spark DF
head(cons_elec_df)
printSchema(cons_elec_df)

# Select the column 'jour' of the Spark DF
print("-----Extract the column jour...")
head(select(cons_elec_df, cons_elec_df$jour))

# Create a new column 'an',
# whose values are extract the year from the column 'jour' of the Spark DF 'cons_elec_df
print("-----Add a new column which extracts year from the column jour...")
mutate(cons_elec_df, cons_elec_df$an <- year(cons_elec_df$jour))
head(cons_elec_df)

# Group the Spark DF by year and by category of client,
# then count the sum of consommation of electricity
print("-----Group by year and by category of client")
groupedByYear <- agg(groupBy(cons_elec_df, "an", "cate_client"), cons_sum = sum(cons_elec_df$cons_elect))
head(groupedByYear)

#Sort the grouped Spark DF by year and category of client in an ascending year order
print("-----Sort the grouped by year DF in an ascending order of year")
sortedGroupedByYear <- arrange(groupedByYear, groupedByYear$an)
head(sortedGroupedByYear)

# Group the Spark DF by category of client,
# then count the sum of consommation of electricity
print("-----Group by the category of client")
groupedByCateClient <- agg(groupBy( cons_elec_df, "cate_client"), cons_sum = sum(cons_elec_df$cons_elect))
collect(groupedByCateClient)

# Count the sum of consommation of electricity for each category of client in 2017
print("-----Sum of consommation of electricity for each category of client in 2017")
groupByCateClient2017 <- agg(groupBy( filter(cons_elec_df, cons_elec_df$an == 2017), cons_elec_df$cate_client, cons_elec_df$an), cons_sum = sum(cons_elec_df$cons_elect))
collect(groupByCateClient2017)

print("--------------END of APPLICATION----------------")