############################################################################
## SparkR: From a CSV to a SparkR DataFrame ##
## Maing, 12-2018 ##
############################################################################
## Objective: Read data about the electric consumation from a CSV file,
## create a Spark DataFrame from the file.
## * Initiate SparkR
## * Set up command line arguments (a script and an input file)
## * Create a function which creates a Spark DataFrame from a file
## * Create a Spark DataFrame from an input files

## How to run the script: "spark-submit script_path input_path"
## e.g. spark-submit cons_elect.R /Users/nguyen/maing/training/r/data/cons_elec.csv


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

######## CREATE DF FROM A FILE ##################

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
cons_elec <- getDF(inputFile, customSchema)

# Analyze the dataframe
print("The number of rows of cons_elec...")
(numberOfRows <- nrow(cons_elec))
print("The number of columns of cons_elec...")
(numberOfCols <- ncol(cons_elec))
print("The dimension of cons_elec...")
(dimensions_cons_elec <- dim(cons_elec))
print("The schema of cons_elec...")
(schema <- printSchema(cons_elec))
