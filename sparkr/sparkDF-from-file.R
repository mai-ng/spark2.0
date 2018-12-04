############################################################################
## SparkR: From CSV to SparkR DataFrame, working on a json setting file ##
## Maing, 12-2018 ##
############################################################################
## Objective: Read a list of input files whose paths are defined a json file,
#### then create Spark Dataframes from these files with a predefined schema.
## * Initiate SparkR
## * Set up command line arguments
## * Read a json file
## * Create a function which gets a schema from a json file
## * Create a function which creates a Spark DataFrame from a file
## * Create a function which creates a Spark DataFrame from a file with a predefined properties
## (header, source, sep, schema)
## * Create a list of Spark DataFrame from a list of input files

## How to run the script: "spark-submit script_file_path setting_file_path"
### e.g. spark-submit sparkr/sparkDF-from-file.R sparkr/settings.json

# Load libraries
library(SparkR)
library(jsonlite)

# Initiate a Spark Session
sparkR.session()

# Set up command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Not enough arguments, Please specify the script following by the setting file.\n", call.=FALSE)
} else {
  confPath = args[1]
}

# Read the configuration file
exConf <- fromJSON(toString(confPath))

# Create a schema from a json file
getSchema <- function(argschema){
  mList <- list()
  for(i in 1:nrow(argschema$schema)){
    sf <- structField(argschema$schema[i,"name"],argschema$schema[i,"type"])
    mList <- c(mList, list(sf))
  }
  customSchema <- do.call(structType, mList)
  return(customSchema)
}

# Create a Spark Dataframe from a file
getDF <- function(filePath, argschema, argsource = "csv", argheader = "true", argsep = ";"){
  if(is.null(argschema)){
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, inferSchema = "true")
  }else{
    df <- read.df(filePath, source = argsource, header = argheader, sep = argsep, schema = argschema)
  }
  return(df)
}

# Process a file
process <- function(argfile, argconf){
  df <- getDF(argfile, getSchema(argconf["schema"]), toString(argconf["format"]), toString(argconf["header"]), toString(argconf["sep"]))
  print("Create a DF from the file: ")
  print(argfile)
  print("Schema of the DF: ")
  printSchema(df)
  return(df)
}
# Parse a list of input files defined in a json file
  for(file in exConf["inputs"]){
    process(file, exConf)
  }
