
# Working with data stored in WASB

Azure Storage Blob (WASB) is used as the storage account associated with an HDInsight cluster. An HDInsight cluster can have a default storage and additional storage. The URL to access the cluster storage is:

    wasb[s]://<container_name>@<storage_account_name>.blob.core.windows.net/<path>
    
The URL to access only the default storage is:

    wasb[s]:///<path>

This notebook provides examples of how to read data from WASB into a Spark context and then perform operations on that data. The notebook also provides examples of how to write the output of Spark jobs directly into a WASB location.


-------
## Read data from WASB into Spark

The examples below read from the default storage account associated with your Spark cluster so the URL used in the examples is `wasb:///<path>`. However, you can also read from an additional storage account with the following syntax:

    wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>

----------
## Notebook setup

When using PySpark kernel notebooks on HDInsight, there is no need to create a SparkContext or a SparkSession; a SparkSession which has the SparkContext is created for you automatically when you run the first code cell, and you'll be able to see the progress printed. The contexts are created with the following variable names:
- SparkSession (spark)

To run the cells below, place the cursor in the cell and then press **SHIFT + ENTER**.

### Create an RDD of strings


```pyspark
# textLines is an RDD of strings
textLines = spark.sparkContext.textFile('wasb:///example/data/gutenberg/ulysses.txt')
```

### Create an RDD of key-value pairs


```pyspark
# seqFile is an RDD of key-value pairs
seqFile = spark.sparkContext.sequenceFile('wasb:///example/data/people.seq')
```

### Create a dataframe from parquet files

Create a dataframe from an input parquet file. For more information about parquet files, see [here](http://spark.apache.org/docs/2.0.0/sql-programming-guide.html#parquet-files).


```pyspark
# parquetFile is a dataframe that matches the schema of the input parquet file
parquetFile = spark.read.parquet('wasb:///example/data/people.parquet')
```

### Create a dataframe from JSON document

Create a dataframe that matches the schema of the input JSON document.


```pyspark
# jsonFile is a dataframe that matches the schema of the input JSON file
jsonFile = spark.read.json('wasb:///example/data/people.json')
```

### Create an dataframe from CSV files

Create a dataframe from a CSV file with headers. Spark can automatically infer its schema.


```pyspark
# csvFile is an dataframe that matches the schema of the input CSV file
csvFile = spark.read.csv('wasb:///HdiSamples/HdiSamples/SensorSampleData/hvac/HVAC.csv', header=True, inferSchema=True)
```

------
## Write data from Spark to WASB in different formats

The examples below show you how to write output data from Spark directly into the storage accounts associated with your Spark cluster. If you are writing to the default storage account, you can provide the output path like this:

    wasb[s]:///<path>

If you are writing to additional storage accounts associated with the cluster, you must provide the output path like this:

    wasb[s]://<container_name>@<storage_account_name>.blob.core.windows.net/<path>

### Save an RDD as text files

If you have an RDD, you can convert it to a text file like the following:


```pyspark
# textLines is an RDD converted into a text file
textLines.saveAsTextFile('wasb:///example/data/gutenberg/ulysses2py.txt')
```

### Save a dataframe as text files

If you have a dataframe that you want to save as a text file, you must first convert it to an RDD and then save that RDD as a text file.


```pyspark
parquetRDD = parquetFile.rdd
parquetRDD.saveAsTextFile('wasb:///example/data/peoplepy.txt')
# parquetFile is a dataframe converted into RDD. parquetRDD is then converted into a text file
```

### Save a dataframe as Parquet, JSON or CSV

If you have a dataframe, you can save it to Parquet or JSON with the `.write.parquet()`, `.write.json()` and `.write.csv()` methods respectively.

Dataframes can be saved in any format, regardless of the input format.


```pyspark
parquetFile.write.json('wasb:///example/data/people3py.json')
csvFile.write.parquet('wasb:///example/data/people3py.parquet')
jsonFile.write.csv('wasb:///example/data/people3py.csv')
```

If you have an RDD and want to save it as a parquet file or JSON file, you'll have to 
convert it to a dataframe. See [Interoperating with RDDs](http://spark.apache.org/docs/2.0.0/sql-programming-guide.html#interoperating-with-rdds) for more information.

### Save an RDD of key-value pairs as a sequence file


```pyspark
# If your RDD isn't made up of key-value pairs then you'll get a runtime error
seqFile.saveAsSequenceFile('wasb:///example/data/people2py.seq')
```
