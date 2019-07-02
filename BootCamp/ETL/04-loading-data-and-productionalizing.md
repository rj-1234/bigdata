
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Loading Data and Productionalizing

Apache Spark&trade; allow you to productionalize code by scheduling notebooks for regular execution.
## In this lesson you:
* Load data using the Apache Parquet format
* Automate a pipeline using `AWS Data Pipeline`


## Introductory Productionalizing

Incorporating notebooks into production workflows will be covered in detail in later courses. This lesson focuses on two aspects of productionalizing: Parquet as a best practice for loading data from ETL jobs and scheduling jobs.

In the road map for ETL, this is the **Load and Automate** step:

<img src="../../resources/ETL-Process-4.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

## Writing Parquet

BLOB stores like S3 and the Azure Blob are the data storage option of choice on Spark, and Parquet is the storage format of choice.  [Apache Parquet](https://parquet.apache.org/documentation/latest/) is a highly efficient, column-oriented data format that shows massive performance increases over other options such as CSV. For instance, Parquet compresses data repeated in a given column and preserves the schema from a write.

When writing data to S3, the best practice is to use Parquet.

Run the following cell to create the lab environment:


```python
#MODE = "LOCAL"
MODE = "CLUSTER"

import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from matplotlib import interactive
interactive(True)
import matplotlib.pyplot as plt
%matplotlib inline
import json
import math
import numbers
import numpy as np
import plotly
plotly.offline.init_notebook_mode(connected=True)

sys.path.insert(0,'../../src')
from settings import *

try:
    fh = open('../../libs/pyspark24_py36.zip', 'r')
except FileNotFoundError:
    !aws s3 cp s3://yuan.intellinum.co/bins/pyspark24_py36.zip ../../libs/pyspark24_py36.zip

try:
    spark.stop()
    print("Stopped a SparkSession")
except Exception as e:
    print("No existing SparkSession detected")
    print("Creating a new SparkSession")

SPARK_DRIVER_MEMORY= "1G"
SPARK_DRIVER_CORE = "1"
SPARK_EXECUTOR_MEMORY= "1G"
SPARK_EXECUTOR_CORE = "1"
SPARK_EXECUTOR_INSTANCES = 12



conf = None
if MODE == "LOCAL":
    os.environ["PYSPARK_PYTHON"] = "/home/yuan/anaconda3/envs/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_04-loading-data-and-productionalizing").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_04-loading-data-and-productionalizing").\
            setMaster('yarn-client').\
            set('spark.executor.cores', SPARK_EXECUTOR_CORE).\
            set('spark.executor.memory', SPARK_EXECUTOR_MEMORY).\
            set('spark.driver.cores', SPARK_DRIVER_CORE).\
            set('spark.driver.memory', SPARK_DRIVER_MEMORY).\
            set("spark.executor.instances", SPARK_EXECUTOR_INSTANCES).\
            set('spark.sql.files.ignoreCorruptFiles', 'true').\
            set('spark.yarn.dist.archives', '../../libs/pyspark24_py36.zip#MN').\
            set('spark.sql.shuffle.partitions', '5000').\
            set('spark.default.parallelism', '5000').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1'). \
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', 's3://yuan.intellinum.co/bins/mysql-connector-java-5.1.45-bin.jar')
        

spark = SparkSession.builder.\
    config(conf=conf).\
    getOrCreate()


sc = spark.sparkContext

sc.addPyFile('../../src/settings.py')

sc=spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")

def display(df, limit=10):
    return df.limit(limit).toPandas()

def dfTest(id, expected, result):
    assert str(expected) == str(result), "{} does not equal expected {}".format(result, expected)
```


        <script type="text/javascript">
        window.PlotlyConfig = {MathJaxConfig: 'local'};
        if (window.MathJax) {MathJax.Hub.Config({SVG: {font: "STIX-Web"}});}
        if (typeof require !== 'undefined') {
        require.undef("plotly");
        requirejs.config({
            paths: {
                'plotly': ['https://cdn.plot.ly/plotly-latest.min']
            }
        });
        require(['plotly'], function(Plotly) {
            window._Plotly = Plotly;
        });
        }
        </script>
        


    No existing SparkSession detected
    Creating a new SparkSession


Import Chicago crime data.


```python
crimeDF = (spark.read
  .option("delimiter", "\t")
  .option("header", True)
  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
  .option("inferSchema", True)
  .csv("s3a://data.intellinum.co/bootcamp/common/Chicago-Crimes-2018.csv")
)
display(crimeDF)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ID</th>
      <th>Case Number</th>
      <th>Date</th>
      <th>Block</th>
      <th>IUCR</th>
      <th>Primary Type</th>
      <th>Description</th>
      <th>Location Description</th>
      <th>Arrest</th>
      <th>Domestic</th>
      <th>...</th>
      <th>Ward</th>
      <th>Community Area</th>
      <th>FBI Code</th>
      <th>X Coordinate</th>
      <th>Y Coordinate</th>
      <th>Year</th>
      <th>Updated On</th>
      <th>Latitude</th>
      <th>Longitude</th>
      <th>Location</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>23811</td>
      <td>JB141441</td>
      <td>2018-01-05 01:10:00</td>
      <td>118XX S INDIANA AVE</td>
      <td>0110</td>
      <td>HOMICIDE</td>
      <td>FIRST DEGREE MURDER</td>
      <td>VACANT LOT</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>9</td>
      <td>53</td>
      <td>01A</td>
      <td>1179707.0</td>
      <td>1826280.0</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>41.678585</td>
      <td>-87.617838</td>
      <td>(41.678585145, -87.617837834)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11228589</td>
      <td>JB148990</td>
      <td>2018-01-23 09:00:00</td>
      <td>072XX S VERNON AVE</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>OTHER</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>6</td>
      <td>69</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11228563</td>
      <td>JB148931</td>
      <td>2018-01-31 10:12:00</td>
      <td>040XX N KEYSTONE AVE</td>
      <td>1154</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT $300 AND UNDER</td>
      <td>APARTMENT</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>39</td>
      <td>16</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11228555</td>
      <td>JB148885</td>
      <td>2018-01-01 14:00:00</td>
      <td>017XX W CONGRESS PKWY</td>
      <td>0820</td>
      <td>THEFT</td>
      <td>$500 AND UNDER</td>
      <td>HOSPITAL BUILDING/GROUNDS</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>2</td>
      <td>28</td>
      <td>06</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11228430</td>
      <td>JB148675</td>
      <td>2018-01-27 21:00:00</td>
      <td>061XX S EBERHART AVE</td>
      <td>0560</td>
      <td>ASSAULT</td>
      <td>SIMPLE</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>True</td>
      <td>...</td>
      <td>20</td>
      <td>42</td>
      <td>08A</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11228401</td>
      <td>JB148683</td>
      <td>2018-01-02 12:00:00</td>
      <td>038XX N SAWYER AVE</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>33</td>
      <td>16</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>11228347</td>
      <td>JB148599</td>
      <td>2018-01-28 19:00:00</td>
      <td>008XX E 45TH ST</td>
      <td>0620</td>
      <td>BURGLARY</td>
      <td>UNLAWFUL ENTRY</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>4</td>
      <td>39</td>
      <td>05</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>11228291</td>
      <td>JB148591</td>
      <td>2018-01-10 16:45:00</td>
      <td>010XX E 53RD ST</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>4</td>
      <td>41</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>11228287</td>
      <td>JB148482</td>
      <td>2018-01-03 15:45:00</td>
      <td>0000X W C1 ST</td>
      <td>0810</td>
      <td>THEFT</td>
      <td>OVER $500</td>
      <td>AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>41</td>
      <td>76</td>
      <td>06</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>11228268</td>
      <td>JB148558</td>
      <td>2018-01-04 16:00:00</td>
      <td>044XX S MICHIGAN AVE</td>
      <td>2825</td>
      <td>OTHER OFFENSE</td>
      <td>HARASSMENT BY TELEPHONE</td>
      <td>APARTMENT</td>
      <td>False</td>
      <td>True</td>
      <td>...</td>
      <td>3</td>
      <td>38</td>
      <td>26</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>10 rows × 22 columns</p>
</div>



Rename the columns in `CrimeDF` so there are no spaces or invalid characters. This is required by Spark and is a best practice.  Use camel case.


```python
cols = crimeDF.columns
titleCols = [''.join(j for j in i.title() if not j.isspace()) for i in cols]
camelCols = [column[0].lower()+column[1:] for column in titleCols]

crimeRenamedColsDF = crimeDF.toDF(*camelCols)
display(crimeRenamedColsDF)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>caseNumber</th>
      <th>date</th>
      <th>block</th>
      <th>iucr</th>
      <th>primaryType</th>
      <th>description</th>
      <th>locationDescription</th>
      <th>arrest</th>
      <th>domestic</th>
      <th>...</th>
      <th>ward</th>
      <th>communityArea</th>
      <th>fbiCode</th>
      <th>xCoordinate</th>
      <th>yCoordinate</th>
      <th>year</th>
      <th>updatedOn</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>location</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>23811</td>
      <td>JB141441</td>
      <td>2018-01-05 01:10:00</td>
      <td>118XX S INDIANA AVE</td>
      <td>0110</td>
      <td>HOMICIDE</td>
      <td>FIRST DEGREE MURDER</td>
      <td>VACANT LOT</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>9</td>
      <td>53</td>
      <td>01A</td>
      <td>1179707.0</td>
      <td>1826280.0</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>41.678585</td>
      <td>-87.617838</td>
      <td>(41.678585145, -87.617837834)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>11228589</td>
      <td>JB148990</td>
      <td>2018-01-23 09:00:00</td>
      <td>072XX S VERNON AVE</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>OTHER</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>6</td>
      <td>69</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>11228563</td>
      <td>JB148931</td>
      <td>2018-01-31 10:12:00</td>
      <td>040XX N KEYSTONE AVE</td>
      <td>1154</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT $300 AND UNDER</td>
      <td>APARTMENT</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>39</td>
      <td>16</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>11228555</td>
      <td>JB148885</td>
      <td>2018-01-01 14:00:00</td>
      <td>017XX W CONGRESS PKWY</td>
      <td>0820</td>
      <td>THEFT</td>
      <td>$500 AND UNDER</td>
      <td>HOSPITAL BUILDING/GROUNDS</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>2</td>
      <td>28</td>
      <td>06</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11228430</td>
      <td>JB148675</td>
      <td>2018-01-27 21:00:00</td>
      <td>061XX S EBERHART AVE</td>
      <td>0560</td>
      <td>ASSAULT</td>
      <td>SIMPLE</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>True</td>
      <td>...</td>
      <td>20</td>
      <td>42</td>
      <td>08A</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11228401</td>
      <td>JB148683</td>
      <td>2018-01-02 12:00:00</td>
      <td>038XX N SAWYER AVE</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>33</td>
      <td>16</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>11228347</td>
      <td>JB148599</td>
      <td>2018-01-28 19:00:00</td>
      <td>008XX E 45TH ST</td>
      <td>0620</td>
      <td>BURGLARY</td>
      <td>UNLAWFUL ENTRY</td>
      <td>RESIDENCE</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>4</td>
      <td>39</td>
      <td>05</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>11228291</td>
      <td>JB148591</td>
      <td>2018-01-10 16:45:00</td>
      <td>010XX E 53RD ST</td>
      <td>1153</td>
      <td>DECEPTIVE PRACTICE</td>
      <td>FINANCIAL IDENTITY THEFT OVER $ 300</td>
      <td>None</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>4</td>
      <td>41</td>
      <td>11</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>11228287</td>
      <td>JB148482</td>
      <td>2018-01-03 15:45:00</td>
      <td>0000X W C1 ST</td>
      <td>0810</td>
      <td>THEFT</td>
      <td>OVER $500</td>
      <td>AIRPORT TERMINAL LOWER LEVEL - NON-SECURE AREA</td>
      <td>False</td>
      <td>False</td>
      <td>...</td>
      <td>41</td>
      <td>76</td>
      <td>06</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>11228268</td>
      <td>JB148558</td>
      <td>2018-01-04 16:00:00</td>
      <td>044XX S MICHIGAN AVE</td>
      <td>2825</td>
      <td>OTHER OFFENSE</td>
      <td>HARASSMENT BY TELEPHONE</td>
      <td>APARTMENT</td>
      <td>False</td>
      <td>True</td>
      <td>...</td>
      <td>3</td>
      <td>38</td>
      <td>26</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>2018</td>
      <td>2018-01-12 15:49:14</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>10 rows × 22 columns</p>
</div>



Write to Parquet by calling the following method on a DataFrame: `.write.parquet("s3://temp.intellinum.co/YOUR_USERNAME/<destination>.parquet")`.

Specify the write mode (for example, `overwrite` or `append`) using `.mode()`.

Write to `s3://temp.intellinum.co/YOUR_USERNAME`, a directory backed by the Azure Blob or S3 available to all Bootcamp clusters. If your `s3://temp.intellinum.co/YOUR_USERNAME` directory is full, clear contents using `aws s3 rm --recursive s3://temp.intellinum.co/YOUR_USERNAME`.

[See the documentation for additional specifications.](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=parquet#pyspark.sql.DataFrameWriter.parquet)


```python
# TODO
YOUR_USER_NAME = 'rajeev'
```


```python
crimeRenamedColsDF.write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_USER_NAME + "/crime.parquet")
```

Review how this command writes the Parquet file. An advantage of Parquet is that, unlike a CSV file which is normally a single file, Parquet is distributed so each partition of data in the cluster writes to its own "part". Notice the different log data included in this directory.

Write other file formats in this same way (for example, `.write.csv("s3://temp.intellinum.co/YOUR_USERNAME/<destination>.parquet")`)


```python
!aws s3 ls {"s3://temp.intellinum.co/%s/crime.parquet/" % YOUR_USER_NAME}

```

    2019-06-11 22:10:15          0 _SUCCESS
    2019-06-11 22:10:13    1213833 part-00000-7cc1f87c-11d0-4912-88d1-35c5195fa9ad-c000.snappy.parquet
    2019-06-11 22:10:14     320946 part-00001-7cc1f87c-11d0-4912-88d1-35c5195fa9ad-c000.snappy.parquet


Use the `repartition` DataFrame method to repartition the data to limit the number of separate parts.

What appears to the user as a single DataFrame is actually data distributed across a cluster.  Each cluster holds _partitions_, or parts, of the data.  By repartitioning, we define how many different parts of our data to have.


```python
crimeRenamedColsDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_USER_NAME + "/crimeRepartitioned.parquet")


```

Now look at how many parts are in the new folder. You have one part for each partition. Since you repartitioned the DataFrame with a value of `1`, now all the data is in `part-00000`.


```python
!aws s3 ls {"s3://temp.intellinum.co/%s/crimeRepartitioned.parquet/" % YOUR_USER_NAME}

```

    2019-06-11 22:10:27          0 _SUCCESS
    2019-06-11 22:10:26    1484346 part-00000-e1a2cd2c-b6c1-4fe7-a1cd-aae7e156546c-c000.snappy.parquet


### Automate by Scheduling a Job

Scheduling a job with [AWS Data Pipeline](https://aws.amazon.com/datapipeline/) allows you to perform a batch process at a regular interval. Schedule email updates for successful completion and error logs.

**Note:** This part of the course is still under construction. Stay tuned


## Review

**Question:** What is the recommended storage format to use with Spark?
**Answer:** Apache Parquet is a highly optimized solution for data storage and is the recommended option for storage where possible.  In addition to offering benefits like compression, it's distributed, so a given partition of data writes to its own file, enabling parallel reads and writes. Formats like CSV are prone to corruption since a single missing comma could corrupt the data. Also, the data cannot be parallelized.

**Question:** How do you schedule a regularly occurring task in AWS?
**Answer:** AWS Data Pipeline allows for job automation.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
