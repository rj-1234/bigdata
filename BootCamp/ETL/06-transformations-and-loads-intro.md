
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

## ETL Part 2: Transformations and Loads

In this course, Data Engineers apply data transformation and extraction best practices such as user-defined functions, efficient table joins, and parallel database writes.  
By the end of this course, you will transform complex data with custom functions, load it into a target database, and navigate Spark documents to source solutions.

**The part-2 course includes the following lessons:**
1. Course Overview and Setup
1. Common Transformations
1. User Defined Functions
1. Advanced UDFs
1. Joins and Lookup Tables
1. Database Writes
1. Table Management
1. Capstone Project: Custom Transformations, Aggregating and Loading

### Raw, Query, and Summary Tables

A number of different terms describe the movement of data through an ETL pipeline. For course purposes, data begins in the pipeline with **raw tables.** This refers to data that arrives in the pipeline, conforms to a schema, and does not include any sort of parsing or aggregation.

Raw tables are then parsed into query-ready tables, known as **query tables.**  Query tables might populate a relational data model for ad hoc (OLAP) or online (OLTP) queries, but generally do not include any sort of aggregation such as daily averages.  Put another way, query tables are cleaned and filtered data.

Finally, **summary tables** are business level aggregates often used for reporting and dashboarding. This includes aggregations such as daily active website visitors.

It is a good idea to preserve raw tables because it lets you adapt to future needs not considered in the original data model or correct parsing errors. This pipeline ensures data engineers always have access to the data they need, while reducing the barriers to common data access patterns. Data becomes more valuable and easier to access in each subsequent stage of the pipeline.  

<div><img src="../../resources/gold-silver-bronze.png" style="height: 400px; margin: 20px"/></div>

# Common Transformations

Apache Spark&trade; allow you to manipulate data with built-in functions that accommodate common design patterns.

## In this lesson you:
* Apply built-in functions to manipulate data
* Define logic to handle null values
* Deduplicate a data set

### Transformations in ETL

The goal of transformations in ETL is to transform raw data in order to populate a data model.  The most common models are **relational models** and **snowflake (or star) schemas,** though other models such as query-first modeling also exist. Relational modeling entails distilling your data into efficient tables that you can join back together. A snowflake model is generally used in data warehousing where a fact table references any number of related dimension tables. Regardless of the model you use, the ETL approach is generally the same.

Transforming data can range in complexity from simply parsing relevant fields to handling null values without affecting downstream operations and applying complex conditional logic.  Common transformations include:<br><br>

* Normalizing values
* Imputing null or missing data
* Deduplicating data
* Performing database rollups
* Exploding arrays
* Pivoting DataFrames

<div><img src="../../resources/data-models.png" style="height: 400px; margin: 20px"/></div>

### Built-In Functions

Built-in functions offer a range of performant options to manipulate data. This includes options familiar to:<br><br>

1. SQL users such as `.select()` and `.groupBy()`
2. Python, Scala and R users such as `max()` and `sum()`
3. Data warehousing options such as `rollup()` and `cube()`

**Hint:** For more depth on built-in functions, see  <a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html" target="_blank">Spark Built-in functions API doc</a>.

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
    !aws s3 cp s3://devops.intellinum.co/bins/pyspark24_py36.zip ../../libs/pyspark24_py36.zip

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
            setAppName("pyspark_etl_06-transformations-and-loads-intro-rajeev").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_06-transformations-and-loads-intro-rajeev").\
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
            set('spark.jars', 's3://devops.intellinum.co/bins/mysql-connector-java-5.1.45-bin.jar')
        

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


### Normalizing Data

Normalizing refers to different practices including restructuring data in normal form to reduce redundancy, and scaling data down to a small, specified range. For this case, bound a range of integers between 0 and 1.

Start by taking a DataFrame of a range of integers


```python
integerDF = spark.range(1000, 10000)

display(integerDF)
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1001</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1002</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1003</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1004</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1005</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1006</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1007</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1008</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1009</td>
    </tr>
  </tbody>
</table>
</div>



**Hint:** To normalize these values between 0 and 1, subtract the minimum and divide by the maximum, minus the minimum.

<a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=minmaxscaler#pyspark.ml.feature.MinMaxScaler" target="_blank">Also see the built-in class `MinMaxScaler`</a>


```python
from pyspark.sql.functions import col, max, min

colMin = integerDF.select(min("id")).first()[0]
colMax = integerDF.select(max("id")).first()[0]

normalizedIntegerDF = (integerDF
  .withColumn("normalizedValue", (col("id") - colMin) / (colMax - colMin) )
)

display(normalizedIntegerDF)
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
      <th>normalizedValue</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1001</td>
      <td>0.000111</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1002</td>
      <td>0.000222</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1003</td>
      <td>0.000333</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1004</td>
      <td>0.000444</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1005</td>
      <td>0.000556</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1006</td>
      <td>0.000667</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1007</td>
      <td>0.000778</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1008</td>
      <td>0.000889</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1009</td>
      <td>0.001000</td>
    </tr>
  </tbody>
</table>
</div>



### Imputing Null or Missing Data

Null values refer to unknown or missing data as well as irrelevant responses. Strategies for dealing with this scenario include:<br><br>

* **Dropping these records:** Works when you do not need to use the information for downstream workloads
* **Adding a placeholder (e.g. `-1`):** Allows you to see missing data later on without violating a schema
* **Basic imputing:** Allows you to have a "best guess" of what the data could have been, often by using the mean of non-missing data
* **Advanced imputing:** Determines the "best guess" of what data should be using more advanced strategies such as clustering machine learning algorithms or oversampling techniques 

**Hint:** <a href="http://spark.apache.org/docs/latest/api/python/pyspark.ml.html?highlight=imputer#pyspark.ml.feature.Imputer" target="_blank">Also see the built-in class `Imputer`</a>

Take a look at the following DataFrame, which has missing values.


```python
corruptDF = spark.createDataFrame([
  (11, 66, 5),
  (12, 68, None),
  (1, None, 6),
  (2, 72, 7)], 
  ["hour", "temperature", "wind"]
)

display(corruptDF)
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
      <th>hour</th>
      <th>temperature</th>
      <th>wind</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11</td>
      <td>66.0</td>
      <td>5.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12</td>
      <td>68.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>NaN</td>
      <td>6.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>72.0</td>
      <td>7.0</td>
    </tr>
  </tbody>
</table>
</div>



Drop any records that have null values.


```python
corruptDroppedDF = corruptDF.dropna("any", subset=['wind', 'temperature'])

display(corruptDroppedDF)
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
      <th>hour</th>
      <th>temperature</th>
      <th>wind</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11</td>
      <td>66</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>72</td>
      <td>7</td>
    </tr>
  </tbody>
</table>
</div>



Impute values with the mean.


```python
corruptImputedDF = corruptDF.na.fill({"temperature": 68, "wind": 6})

display(corruptImputedDF)
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
      <th>hour</th>
      <th>temperature</th>
      <th>wind</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>11</td>
      <td>66</td>
      <td>5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12</td>
      <td>68</td>
      <td>6</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>68</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>72</td>
      <td>7</td>
    </tr>
  </tbody>
</table>
</div>



### Deduplicating Data

Duplicate data comes in many forms. The simple case involves records that are complete duplicates of another record. The more complex cases involve duplicates that are not complete matches, such as matches on one or two columns or "fuzzy" matches that account for formatting differences or other non-exact matches. 

Take a look at the following DataFrame that has duplicate values.


```python
duplicateDF = spark.createDataFrame([
  (15342, "Conor", "red"),
  (15342, "conor", "red"),
  (12512, "Dorothy", "blue"),
  (5234, "Doug", "aqua")], 
  ["id", "name", "favorite_color"]
)

display(duplicateDF)
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
      <th>name</th>
      <th>favorite_color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15342</td>
      <td>Conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>1</th>
      <td>15342</td>
      <td>conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>2</th>
      <td>12512</td>
      <td>Dorothy</td>
      <td>blue</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5234</td>
      <td>Doug</td>
      <td>aqua</td>
    </tr>
  </tbody>
</table>
</div>



Drop duplicates on `id` and `favorite_color`.


```python
duplicateDedupedDF = duplicateDF.dropDuplicates(["id", "favorite_color"])

display(duplicateDedupedDF)
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
      <th>name</th>
      <th>favorite_color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>12512</td>
      <td>Dorothy</td>
      <td>blue</td>
    </tr>
    <tr>
      <th>1</th>
      <td>15342</td>
      <td>conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5234</td>
      <td>Doug</td>
      <td>aqua</td>
    </tr>
  </tbody>
</table>
</div>




```python
duplicateDedupedDF1 = duplicateDF.dropDuplicates(["id"])

display(duplicateDedupedDF1)
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
      <th>name</th>
      <th>favorite_color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15342</td>
      <td>conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12512</td>
      <td>Dorothy</td>
      <td>blue</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5234</td>
      <td>Doug</td>
      <td>aqua</td>
    </tr>
  </tbody>
</table>
</div>




```python
duplicateDedupedDF2 = duplicateDF.dropDuplicates(["name"])

display(duplicateDedupedDF2)
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
      <th>name</th>
      <th>favorite_color</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15342</td>
      <td>Conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>1</th>
      <td>15342</td>
      <td>conor</td>
      <td>red</td>
    </tr>
    <tr>
      <th>2</th>
      <td>12512</td>
      <td>Dorothy</td>
      <td>blue</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5234</td>
      <td>Doug</td>
      <td>aqua</td>
    </tr>
  </tbody>
</table>
</div>



### Other Helpful Data Manipulation Functions

| Function    | Use                                                                                                                        |
|:------------|:---------------------------------------------------------------------------------------------------------------------------|
| `explode()` | Returns a new row for each element in the given array or map                                                               |
| `pivot()`   | Pivots a column of the current DataFrame and perform the specified aggregation                                             |
| `cube()`    | Create a multi-dimensional cube for the current DataFrame using the specified columns, so we can run aggregation on them   |
| `rollup()`  | Create a multi-dimensional rollup for the current DataFrame using the specified columns, so we can run aggregation on them |

## Exercise 1: Deduplicating Data

A common ETL workload involves cleaning duplicated records that don't completely match up.  The source of the problem can be anything from user-generated content to schema evolution and data corruption.  Here, you match records and reduce duplicate records. 

### Step 1: Import and Examine the Data

The file is sitting in `s3://data.intellinum.co/bootcamp/common/dataframes/people-with-dups.txt`.

**Hint:** You have to deal with the header and delimiter.


```python
# TODO
path = "s3://data.intellinum.co/bootcamp/common/dataframes/people-with-dups.txt"
dupedDF = spark.read.csv(path, sep=":", header=True)

```


```python
display(dupedDF)
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
      <th>firstName</th>
      <th>middleName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>birthDate</th>
      <th>salary</th>
      <th>ssn</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Emanuel</td>
      <td>Wallace</td>
      <td>Panton</td>
      <td>M</td>
      <td>1988-03-04</td>
      <td>101255</td>
      <td>935-90-7627</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Eloisa</td>
      <td>Rubye</td>
      <td>Cayouette</td>
      <td>F</td>
      <td>2000-06-20</td>
      <td>204031</td>
      <td>935-89-9009</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Cathi</td>
      <td>Svetlana</td>
      <td>Prins</td>
      <td>F</td>
      <td>2012-12-22</td>
      <td>35895</td>
      <td>959-30-7957</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mitchel</td>
      <td>Andres</td>
      <td>Mozdzierz</td>
      <td>M</td>
      <td>1966-05-06</td>
      <td>55108</td>
      <td>989-27-8093</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Angla</td>
      <td>Melba</td>
      <td>Hartzheim</td>
      <td>F</td>
      <td>1938-07-26</td>
      <td>13199</td>
      <td>935-27-4276</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Rachel</td>
      <td>Marlin</td>
      <td>Borremans</td>
      <td>F</td>
      <td>1923-02-23</td>
      <td>67070</td>
      <td>996-41-8616</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Catarina</td>
      <td>Phylicia</td>
      <td>Dominic</td>
      <td>F</td>
      <td>1969-09-29</td>
      <td>201021</td>
      <td>999-84-8888</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Antione</td>
      <td>Randy</td>
      <td>Hamacher</td>
      <td>M</td>
      <td>2004-03-05</td>
      <td>271486</td>
      <td>917-96-3554</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Madaline</td>
      <td>Shawanda</td>
      <td>Piszczek</td>
      <td>F</td>
      <td>1996-03-17</td>
      <td>183944</td>
      <td>963-87-9974</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Luciano</td>
      <td>Norbert</td>
      <td>Sarcone</td>
      <td>M</td>
      <td>1962-12-14</td>
      <td>73069</td>
      <td>909-96-1669</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
cols = set(dupedDF.columns)

dfTest("ET2-P-02-01-01", 103000, dupedDF.count())
dfTest("ET2-P-02-01-02", True, "salary" in cols and "lastName" in cols)

print("Tests passed!")
```

    Tests passed!


### Step 2: Add Columns to Filter Duplicates

Add columns following to allow you to filter duplicate values.  Add the following:

- `lcFirstName`: first name lower case
- `lcLastName`: last name lower case
- `lcMiddleName`: middle name lower case
- `ssnNums`: social security number without hyphens between numbers

Save the results to `dupedWithColsDF`.

**Hint:** Use the Spark function `lower()`


```python
# TODO
dupedWithColsDF = dupedDF.withColumn("lcFirstName", F.lower(F.col("FirstName"))) \
                            .withColumn("lcLastName", F.lower(F.col("LastName"))) \
                            .withColumn("lcMiddleName", F.lower(F.col("MiddleName"))) \
                            .withColumn("ssnNums", (F.regexp_replace(F.col("ssn"), "-", "")))
```


```python
display(dupedWithColsDF)
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
      <th>firstName</th>
      <th>middleName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>birthDate</th>
      <th>salary</th>
      <th>ssn</th>
      <th>lcFirstName</th>
      <th>lcLastName</th>
      <th>lcMiddleName</th>
      <th>ssnNums</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Emanuel</td>
      <td>Wallace</td>
      <td>Panton</td>
      <td>M</td>
      <td>1988-03-04</td>
      <td>101255</td>
      <td>935-90-7627</td>
      <td>emanuel</td>
      <td>panton</td>
      <td>wallace</td>
      <td>935907627</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Eloisa</td>
      <td>Rubye</td>
      <td>Cayouette</td>
      <td>F</td>
      <td>2000-06-20</td>
      <td>204031</td>
      <td>935-89-9009</td>
      <td>eloisa</td>
      <td>cayouette</td>
      <td>rubye</td>
      <td>935899009</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Cathi</td>
      <td>Svetlana</td>
      <td>Prins</td>
      <td>F</td>
      <td>2012-12-22</td>
      <td>35895</td>
      <td>959-30-7957</td>
      <td>cathi</td>
      <td>prins</td>
      <td>svetlana</td>
      <td>959307957</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Mitchel</td>
      <td>Andres</td>
      <td>Mozdzierz</td>
      <td>M</td>
      <td>1966-05-06</td>
      <td>55108</td>
      <td>989-27-8093</td>
      <td>mitchel</td>
      <td>mozdzierz</td>
      <td>andres</td>
      <td>989278093</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Angla</td>
      <td>Melba</td>
      <td>Hartzheim</td>
      <td>F</td>
      <td>1938-07-26</td>
      <td>13199</td>
      <td>935-27-4276</td>
      <td>angla</td>
      <td>hartzheim</td>
      <td>melba</td>
      <td>935274276</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Rachel</td>
      <td>Marlin</td>
      <td>Borremans</td>
      <td>F</td>
      <td>1923-02-23</td>
      <td>67070</td>
      <td>996-41-8616</td>
      <td>rachel</td>
      <td>borremans</td>
      <td>marlin</td>
      <td>996418616</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Catarina</td>
      <td>Phylicia</td>
      <td>Dominic</td>
      <td>F</td>
      <td>1969-09-29</td>
      <td>201021</td>
      <td>999-84-8888</td>
      <td>catarina</td>
      <td>dominic</td>
      <td>phylicia</td>
      <td>999848888</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Antione</td>
      <td>Randy</td>
      <td>Hamacher</td>
      <td>M</td>
      <td>2004-03-05</td>
      <td>271486</td>
      <td>917-96-3554</td>
      <td>antione</td>
      <td>hamacher</td>
      <td>randy</td>
      <td>917963554</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Madaline</td>
      <td>Shawanda</td>
      <td>Piszczek</td>
      <td>F</td>
      <td>1996-03-17</td>
      <td>183944</td>
      <td>963-87-9974</td>
      <td>madaline</td>
      <td>piszczek</td>
      <td>shawanda</td>
      <td>963879974</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Luciano</td>
      <td>Norbert</td>
      <td>Sarcone</td>
      <td>M</td>
      <td>1962-12-14</td>
      <td>73069</td>
      <td>909-96-1669</td>
      <td>luciano</td>
      <td>sarcone</td>
      <td>norbert</td>
      <td>909961669</td>
    </tr>
  </tbody>
</table>
</div>




```python
dupedWithColsDF.count()
```




    103000




```python
# TEST - Run this cell to test your solution
cols = set(dupedWithColsDF.columns)

dfTest("ET2-P-02-02-01", 103000, dupedWithColsDF.count())
dfTest("ET2-P-02-02-02", True, "lcFirstName" in cols and "lcLastName" in cols)

print("Tests passed!")
```

    Tests passed!


### Step 3: Deduplicate the Data

Deduplicate the data by dropping duplicates of all records except for the original names (first, middle, and last) and the original `ssn`.  Save the result to `dedupedDF`.  Drop the columns you added in step 2.


```python
# TODO
dedupedDF = dupedWithColsDF.dropDuplicates(['lcFirstName', 'lcLastname', 'lcMiddleName', 'ssnNums']) \
                            .drop('lcFirstName', 'lcLastname', 'lcMiddleName', 'ssnNums')
```


```python
display(dedupedDF)
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
      <th>firstName</th>
      <th>middleName</th>
      <th>lastName</th>
      <th>gender</th>
      <th>birthDate</th>
      <th>salary</th>
      <th>ssn</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Alec</td>
      <td>Asa</td>
      <td>Robertshaw</td>
      <td>M</td>
      <td>1994-09-25</td>
      <td>240930</td>
      <td>952-82-4767</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Alton</td>
      <td>Julius</td>
      <td>Raymo</td>
      <td>M</td>
      <td>1997-08-05</td>
      <td>148296</td>
      <td>981-59-2885</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BYRON</td>
      <td>MARCEL</td>
      <td>MADEIRA</td>
      <td>M</td>
      <td>1966-08-29</td>
      <td>298236</td>
      <td>975567246</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Cary</td>
      <td>Anabel</td>
      <td>Blystone</td>
      <td>F</td>
      <td>1967-09-14</td>
      <td>20374</td>
      <td>903-32-1862</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Colin</td>
      <td>Luke</td>
      <td>Mitrani</td>
      <td>M</td>
      <td>1961-05-23</td>
      <td>128139</td>
      <td>926-93-9566</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Deetta</td>
      <td>Phebe</td>
      <td>Pesina</td>
      <td>F</td>
      <td>1948-04-12</td>
      <td>141651</td>
      <td>922-44-6662</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Dulce</td>
      <td>Selena</td>
      <td>Glenny</td>
      <td>F</td>
      <td>1983-12-16</td>
      <td>299570</td>
      <td>981-89-6492</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Erik</td>
      <td>Bruce</td>
      <td>Montono</td>
      <td>M</td>
      <td>1987-03-16</td>
      <td>79318</td>
      <td>940-15-2350</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Jamila</td>
      <td>Coral</td>
      <td>Dunkentell</td>
      <td>F</td>
      <td>1946-04-21</td>
      <td>167113</td>
      <td>915-84-1107</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Kenneth</td>
      <td>Eric</td>
      <td>Allebach</td>
      <td>M</td>
      <td>1985-07-11</td>
      <td>129543</td>
      <td>977-71-9682</td>
    </tr>
  </tbody>
</table>
</div>




```python
dedupedDF.count()
```




    100000




```python
# TEST - Run this cell to test your solution
cols = set(dedupedDF.columns)

dfTest("ET2-P-02-03-01", 100000, dedupedDF.count())
dfTest("ET2-P-02-03-02", 7, len(cols))

print("Tests passed!")
```

    Tests passed!


## Review
**Question:** What built-in functions are available in Spark?  
**Answer:** Built-in functions include SQL functions, common programming language primitives, and data warehousing specific functions.  See the Spark API Docs for more details. (<a href="http://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python</a> or <a href="http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.package" target="_blank">Scala</a>).

**Question:** What's the best way to handle null values?  
**Answer:** The answer depends largely on what you hope to do with your data moving forward. You can drop null values or impute them with a number of different techniques.  For instance, clustering your data to fill null values with the values of nearby neighbors often gives more insight to machine learning models than using a simple mean.

**Question:** What are potential challenges of deduplicating data and imputing null values?  
**Answer:** Challenges include knowing which is the correct record to keep and how to define logic that applies to the root cause of your situation. This decision making process depends largely on how removing or imputing data will affect downstream operations like database queries and machine learning workloads. Knowing the end application of the data helps determine the best strategy to use.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
