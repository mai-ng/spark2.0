############################################################################

# Spark in R

*Maing, 12-2018*

############################################################################

## Objectives:
To work on Spark libraries in R: SparkR, Sparklyr...

## Projects:
- **cons_elect.R**: Read data about the electric consumation from a CSV file, then create a Spark DataFrame from the file.

- **sparkDF-from-file.R**: Start from a json file which contains the properties of a set of input files (a list of input files, a schema, header, file format, separator, ...). We then create Spark Dataframes from these input files with a predefined schema.

- **setting.json**: A config file which defines properties (header, separator, schema, ...) of the DataFrame.

- **cons_elect_processing.R**: Read data about the electric consumation from a CSV file, then create a Spark DataFrame from the file. We process the data using SparkR operations: filter(), select(), groupBy(), agg(), arrange(), year(), sum().