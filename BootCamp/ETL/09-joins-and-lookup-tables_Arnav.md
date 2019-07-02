
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Joins and Lookup Tables

Apache Spark&trade; allows you to join new records to existing tables in an ETL job.

## In this lesson you:
* Join new records to a pre-existing lookup table
* Employ table join best practices relevant to big data environments


### Shuffle and Broadcast Joins

A common use case in ETL jobs involves joining new data to either lookup tables or historical data. You need different considerations to guide this process when working with distributed technologies such as Spark, rather than traditional databases that sit on a single machine.

Traditional databases join tables by pairing values on a given column. When all the data sits in a single database, it often goes unnoticed how computationally expensive row-wise comparisons are.  When data is distributed across a cluster, the expense of joins becomes even more apparent.

**A standard (or shuffle) join** moves all the data on the cluster for each table to a given node on the cluster. This is expensive not only because of the computation needed to perform row-wise comparisons, but also because data transfer across a network is often the biggest performance bottleneck of distributed systems.

By contrast, **a broadcast join** remedies this situation when one DataFrame is sufficiently small. A broadcast join duplicates the smaller of the two DataFrames on each node of the cluster, avoiding the cost of shuffling the bigger DataFrame.

<div><img src="../../resources/shuffle-and-broadcast-joins.png" style="height: 400px; margin: 20px"/></div>

### Lookup Tables

Lookup tables are normally small, historical tables used to enrich new data passing through an ETL pipeline.

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
            setAppName("pyspark_etl_09-joins-and-lookup-tables").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_09-joins-and-lookup-tables-rajeev").\
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


Import a small table that will enrich new data coming into a pipeline.


```python
labelsDF = spark.read.parquet("s3://data.intellinum.co/bootcamp/common/day-of-week")

display(labelsDF)
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
      <th>dow</th>
      <th>longName</th>
      <th>abbreviated</th>
      <th>shortName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Tuesday</td>
      <td>Tue</td>
      <td>Tu</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Wednesday</td>
      <td>Wed</td>
      <td>W</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Thursday</td>
      <td>Thr</td>
      <td>Th</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Friday</td>
      <td>Fri</td>
      <td>F</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Saturday</td>
      <td>Sat</td>
      <td>Sa</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Sunday</td>
      <td>Sun</td>
      <td>Su</td>
    </tr>
  </tbody>
</table>
</div>



Import a larger DataFrame that gives a column to combine back to the lookup table. In this case, use Wikipedia site requests data.


```python
from pyspark.sql.functions import col, date_format

pageviewsDF = (spark.read
  .parquet("s3://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second.parquet/")
  .withColumn("dow", date_format(col("timestamp"), "u").alias("dow"))
)

display(pageviewsDF)
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
      <th>timestamp</th>
      <th>site</th>
      <th>requests</th>
      <th>dow</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015-03-16T00:09:55</td>
      <td>mobile</td>
      <td>1595</td>
      <td>1</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015-03-16T00:10:39</td>
      <td>mobile</td>
      <td>1544</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015-03-16T00:19:39</td>
      <td>desktop</td>
      <td>2460</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015-03-16T00:38:11</td>
      <td>desktop</td>
      <td>2237</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015-03-16T00:42:40</td>
      <td>mobile</td>
      <td>1656</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2015-03-16T00:52:24</td>
      <td>desktop</td>
      <td>2452</td>
      <td>1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2015-03-16T00:54:16</td>
      <td>mobile</td>
      <td>1654</td>
      <td>1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2015-03-16T01:18:11</td>
      <td>mobile</td>
      <td>1720</td>
      <td>1</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2015-03-16T01:30:32</td>
      <td>desktop</td>
      <td>2288</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2015-03-16T01:32:24</td>
      <td>mobile</td>
      <td>1609</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
</div>



Join the two DataFrames together.


```python
pageviewsEnhancedDF = pageviewsDF.join(labelsDF, "dow")

display(pageviewsEnhancedDF)
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
      <th>dow</th>
      <th>timestamp</th>
      <th>site</th>
      <th>requests</th>
      <th>longName</th>
      <th>abbreviated</th>
      <th>shortName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2015-03-16T00:09:55</td>
      <td>mobile</td>
      <td>1595</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2015-03-16T00:10:39</td>
      <td>mobile</td>
      <td>1544</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1</td>
      <td>2015-03-16T00:19:39</td>
      <td>desktop</td>
      <td>2460</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1</td>
      <td>2015-03-16T00:38:11</td>
      <td>desktop</td>
      <td>2237</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>2015-03-16T00:42:40</td>
      <td>mobile</td>
      <td>1656</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>5</th>
      <td>1</td>
      <td>2015-03-16T00:52:24</td>
      <td>desktop</td>
      <td>2452</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>6</th>
      <td>1</td>
      <td>2015-03-16T00:54:16</td>
      <td>mobile</td>
      <td>1654</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>7</th>
      <td>1</td>
      <td>2015-03-16T01:18:11</td>
      <td>mobile</td>
      <td>1720</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>8</th>
      <td>1</td>
      <td>2015-03-16T01:30:32</td>
      <td>desktop</td>
      <td>2288</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1</td>
      <td>2015-03-16T01:32:24</td>
      <td>mobile</td>
      <td>1609</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
    </tr>
  </tbody>
</table>
</div>



Now aggregate the results to see trends by day of the week.

:NOTE: `pageviewsEnhancedDF` is a large DataFrame so it can take a while to process depending on the size of your cluster.


```python
from pyspark.sql.functions import col

aggregatedDowDF = (pageviewsEnhancedDF
  .groupBy(col("dow"), col("longName"), col("abbreviated"), col("shortName"))  
  .sum("requests")                                             
  .withColumnRenamed("sum(requests)", "Requests")
  .orderBy(col("dow"))
)

display(aggregatedDowDF)
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
      <th>dow</th>
      <th>longName</th>
      <th>abbreviated</th>
      <th>shortName</th>
      <th>Requests</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>Monday</td>
      <td>Mon</td>
      <td>M</td>
      <td>2356818845</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>Tuesday</td>
      <td>Tue</td>
      <td>Tu</td>
      <td>1995034884</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>Wednesday</td>
      <td>Wed</td>
      <td>W</td>
      <td>1977615396</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>Thursday</td>
      <td>Thr</td>
      <td>Th</td>
      <td>1931508977</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>Friday</td>
      <td>Fri</td>
      <td>F</td>
      <td>1842512718</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>Saturday</td>
      <td>Sat</td>
      <td>Sa</td>
      <td>1662762048</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>Sunday</td>
      <td>Sun</td>
      <td>Su</td>
      <td>1576726066</td>
    </tr>
  </tbody>
</table>
</div>



### Exploring Broadcast Joins

In joining these two DataFrames together, no type of join was specified.  In order to examine this, look at the physical plan used to return the query. This can be done with the `.explain()` DataFrame method. Look for **BroadcastHashJoin** and/or **BroadcastExchange**.

<div><img src="../../resources/broadcasthashjoin.png" style="height: 400px; margin: 20px"/></div>


```python
aggregatedDowDF.explain()
```

    == Physical Plan ==
    *(4) Sort [dow#15 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(dow#15 ASC NULLS FIRST, 5000)
       +- *(3) HashAggregate(keys=[dow#15, longName#1, abbreviated#2, shortName#3], functions=[sum(cast(requests#10 as bigint))])
          +- Exchange hashpartitioning(dow#15, longName#1, abbreviated#2, shortName#3, 5000)
             +- *(2) HashAggregate(keys=[dow#15, longName#1, abbreviated#2, shortName#3], functions=[partial_sum(cast(requests#10 as bigint))])
                +- *(2) Project [dow#15, requests#10, longName#1, abbreviated#2, shortName#3]
                   +- *(2) BroadcastHashJoin [cast(dow#15 as int)], [dow#0], Inner, BuildRight
                      :- *(2) Project [requests#10, date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)) AS dow#15]
                      :  +- *(2) Filter isnotnull(date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)))
                      :     +- *(2) FileScan parquet [timestamp#8,requests#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<timestamp:string,requests:int>
                      +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
                         +- *(1) Project [dow#0, longName#1, abbreviated#2, shortName#3]
                            +- *(1) Filter isnotnull(dow#0)
                               +- *(1) FileScan parquet [dow#0,longName#1,abbreviated#2,shortName#3] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/day-of-week], PartitionFilters: [], PushedFilters: [IsNotNull(dow)], ReadSchema: struct<dow:int,longName:string,abbreviated:string,shortName:string>


By default, Spark did a broadcast join rather than a shuffle join.  In other words, it broadcast `labelsDF` to the larger `pageviewsDF`, replicating the smaller DataFrame on each node of our cluster.  This avoided having to move the larger DataFrame across the cluster.

Take a look at the broadcast threshold by accessing the configuration settings.


```python
threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
print("Threshold: {0:,}".format( int(threshold) ))
```

    Threshold: 10,485,760


This is the maximize size in bytes for a table that broadcast to worker nodes.  Dropping it to `-1` disables broadcasting.


```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

Now notice the lack of broadcast in the query physical plan.


```python
pageviewsDF.join(labelsDF, "dow").explain()
```

    == Physical Plan ==
    *(5) Project [dow#15, timestamp#8, site#9, requests#10, longName#1, abbreviated#2, shortName#3]
    +- *(5) SortMergeJoin [cast(dow#15 as int)], [dow#0], Inner
       :- *(2) Sort [cast(dow#15 as int) ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(cast(dow#15 as int), 5000)
       :     +- *(1) Project [timestamp#8, site#9, requests#10, date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)) AS dow#15]
       :        +- *(1) Filter isnotnull(date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)))
       :           +- *(1) FileScan parquet [timestamp#8,site#9,requests#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<timestamp:string,site:string,requests:int>
       +- *(4) Sort [dow#0 ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(dow#0, 5000)
             +- *(3) Project [dow#0, longName#1, abbreviated#2, shortName#3]
                +- *(3) Filter isnotnull(dow#0)
                   +- *(3) FileScan parquet [dow#0,longName#1,abbreviated#2,shortName#3] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/day-of-week], PartitionFilters: [], PushedFilters: [IsNotNull(dow)], ReadSchema: struct<dow:int,longName:string,abbreviated:string,shortName:string>


Next reset the original threshold.


```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
```

### Explicitly Broadcasting Tables

There are two ways of telling Spark to explicitly broadcast tables. The first is to change the Spark configuration, which affects all operations. The second is to declare it using the `broadcast()` function in the `functions` package.


```python
from pyspark.sql.functions import broadcast

pageviewsDF.join(broadcast(labelsDF), "dow").explain()
```

    == Physical Plan ==
    *(2) Project [dow#15, timestamp#8, site#9, requests#10, longName#1, abbreviated#2, shortName#3]
    +- *(2) BroadcastHashJoin [cast(dow#15 as int)], [dow#0], Inner, BuildRight
       :- *(2) Project [timestamp#8, site#9, requests#10, date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)) AS dow#15]
       :  +- *(2) Filter isnotnull(date_format(cast(timestamp#8 as timestamp), u, Some(Etc/Universal)))
       :     +- *(2) FileScan parquet [timestamp#8,site#9,requests#10] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<timestamp:string,site:string,requests:int>
       +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
          +- *(1) Project [dow#0, longName#1, abbreviated#2, shortName#3]
             +- *(1) Filter isnotnull(dow#0)
                +- *(1) FileScan parquet [dow#0,longName#1,abbreviated#2,shortName#3] Batched: true, Format: Parquet, Location: InMemoryFileIndex[s3://data.intellinum.co/bootcamp/common/day-of-week], PartitionFilters: [], PushedFilters: [IsNotNull(dow)], ReadSchema: struct<dow:int,longName:string,abbreviated:string,shortName:string>


## Exercise 1: Join a Lookup Table

Join a table that includes country name to a lookup table containing the full country name.

### Step 1: Import the Data

Create the following DataFrames:<br><br>

- `countryLookupDF`: A lookup table with ISO country codes located at `s3a://data.intellinum.co/bootcamp/common/countries/ISOCountryCodes/ISOCountryLookup.parquet`
- `logWithIPDF`: A server log including the results from an IPLookup table located at `s3a://data.intellinum.co/bootcamp/common/EDGAR-Log-20170329/enhanced/logDFwithIP.parquet`


```python
# TODO
path_country_lookup = "s3a://data.intellinum.co/bootcamp/common/countries/ISOCountryCodes/ISOCountryLookup.parquet"
countryLookupDF = spark.read.parquet(path_country_lookup)
countryLookupDF.count()
```




    249




```python
path_log_with_ip = "s3a://data.intellinum.co/bootcamp/common/EDGAR-Log-20170329/enhanced/logDFwithIP.parquet"
logWithIPDF = spark.read.parquet(path_log_with_ip)
logWithIPDF.count()
```




    5000




```python
# TEST - Run this cell to test your solution
dfTest("ET2-P-05-01-01", 249, countryLookupDF.count())
dfTest("ET2-P-05-01-02", 5000, logWithIPDF.count())

print("Tests passed!")
```

    Tests passed!


### Step 2: Broadcast the Lookup Table

Complete the following:<br><br>

- Create a new DataFrame `logWithIPEnhancedDF`
- Get the full country name by performing a broadcast join that broadcasts the lookup table to the server log
- Drop all columns other than `EnglishShortName`


```python
# TODO
display(countryLookupDF)
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
      <th>EnglishShortName</th>
      <th>alpha2Code</th>
      <th>alpha3Code</th>
      <th>numericCode</th>
      <th>ISO31662SubdivisionCode</th>
      <th>independentTerritory</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Afghanistan</td>
      <td>AF</td>
      <td>AFG</td>
      <td>004</td>
      <td>ISO 3166-2:AF</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Åland Islands</td>
      <td>AX</td>
      <td>ALA</td>
      <td>248</td>
      <td>ISO 3166-2:AX</td>
      <td>No</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Albania</td>
      <td>AL</td>
      <td>ALB</td>
      <td>008</td>
      <td>ISO 3166-2:AL</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Algeria</td>
      <td>DZ</td>
      <td>DZA</td>
      <td>012</td>
      <td>ISO 3166-2:DZ</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>4</th>
      <td>American Samoa</td>
      <td>AS</td>
      <td>ASM</td>
      <td>016</td>
      <td>ISO 3166-2:AS</td>
      <td>No</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Andorra</td>
      <td>AD</td>
      <td>AND</td>
      <td>020</td>
      <td>ISO 3166-2:AD</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Angola</td>
      <td>AO</td>
      <td>AGO</td>
      <td>024</td>
      <td>ISO 3166-2:AO</td>
      <td>Yes</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Anguilla</td>
      <td>AI</td>
      <td>AIA</td>
      <td>660</td>
      <td>ISO 3166-2:AI</td>
      <td>No</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Antarctica</td>
      <td>AQ</td>
      <td>ATA</td>
      <td>010</td>
      <td>ISO 3166-2:AQ</td>
      <td>No</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Antigua and Barbuda</td>
      <td>AG</td>
      <td>ATG</td>
      <td>028</td>
      <td>ISO 3166-2:AG</td>
      <td>Yes</td>
    </tr>
  </tbody>
</table>
</div>




```python
display(logWithIPDF)
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
      <th>ip</th>
      <th>date</th>
      <th>time</th>
      <th>zone</th>
      <th>cik</th>
      <th>accession</th>
      <th>extention</th>
      <th>code</th>
      <th>size</th>
      <th>idx</th>
      <th>norefer</th>
      <th>noagent</th>
      <th>find</th>
      <th>crawler</th>
      <th>browser</th>
      <th>IPLookupISO2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101.71.41.ihh</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1437491.0</td>
      <td>0001245105-17-000052</td>
      <td>xslF345X03/primary_doc.xml</td>
      <td>301.0</td>
      <td>687.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>CN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>104.196.240.dda</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1270985.0</td>
      <td>0001188112-04-001037</td>
      <td>.txt</td>
      <td>200.0</td>
      <td>7619.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>2</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0000905148-07-006108</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2727.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>3</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0000905148-08-001993</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2710.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>4</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0001104659-09-046963</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2715.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>5</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1364986.0</td>
      <td>0000914121-06-002243</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2786.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>6</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1364986.0</td>
      <td>0000914121-06-002251</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2784.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>7</th>
      <td>108.240.248.gha</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1540159.0</td>
      <td>0001217160-12-000029</td>
      <td>f332scottlease.htm</td>
      <td>200.0</td>
      <td>49578.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>8</th>
      <td>108.59.8.fef</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>732834.0</td>
      <td>0001209191-15-017349</td>
      <td>xslF345X03/doc4.xml</td>
      <td>301.0</td>
      <td>673.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
    <tr>
      <th>9</th>
      <td>108.91.91.hbc</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1629769.0</td>
      <td>0001209191-17-023204</td>
      <td>.txt</td>
      <td>301.0</td>
      <td>675.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
    </tr>
  </tbody>
</table>
</div>




```python
logWithIPEnhancedDF = logWithIPDF.join(F.broadcast(countryLookupDF), logWithIPDF['IPLookupISO2'] == countryLookupDF['alpha2Code']) \
                                    .drop('alpha2Code', 'alpha3Code', 'numericCode', 'ISO31662SubdivisionCode', 'independentTerritory')
```


```python
display(logWithIPEnhancedDF)
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
      <th>ip</th>
      <th>date</th>
      <th>time</th>
      <th>zone</th>
      <th>cik</th>
      <th>accession</th>
      <th>extention</th>
      <th>code</th>
      <th>size</th>
      <th>idx</th>
      <th>norefer</th>
      <th>noagent</th>
      <th>find</th>
      <th>crawler</th>
      <th>browser</th>
      <th>IPLookupISO2</th>
      <th>EnglishShortName</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>101.71.41.ihh</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1437491.0</td>
      <td>0001245105-17-000052</td>
      <td>xslF345X03/primary_doc.xml</td>
      <td>301.0</td>
      <td>687.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>CN</td>
      <td>China</td>
    </tr>
    <tr>
      <th>1</th>
      <td>104.196.240.dda</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1270985.0</td>
      <td>0001188112-04-001037</td>
      <td>.txt</td>
      <td>200.0</td>
      <td>7619.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>2</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0000905148-07-006108</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2727.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>3</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0000905148-08-001993</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2710.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>4</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1059376.0</td>
      <td>0001104659-09-046963</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2715.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>5</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1364986.0</td>
      <td>0000914121-06-002243</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2786.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>6</th>
      <td>107.23.85.jfd</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1364986.0</td>
      <td>0000914121-06-002251</td>
      <td>-index.htm</td>
      <td>200.0</td>
      <td>2784.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>7</th>
      <td>108.240.248.gha</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1540159.0</td>
      <td>0001217160-12-000029</td>
      <td>f332scottlease.htm</td>
      <td>200.0</td>
      <td>49578.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>8</th>
      <td>108.59.8.fef</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>732834.0</td>
      <td>0001209191-15-017349</td>
      <td>xslF345X03/doc4.xml</td>
      <td>301.0</td>
      <td>673.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
    <tr>
      <th>9</th>
      <td>108.91.91.hbc</td>
      <td>2017-03-29</td>
      <td>00:00:00</td>
      <td>0.0</td>
      <td>1629769.0</td>
      <td>0001209191-17-023204</td>
      <td>.txt</td>
      <td>301.0</td>
      <td>675.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>0.0</td>
      <td>10.0</td>
      <td>0.0</td>
      <td>None</td>
      <td>US</td>
      <td>United States of America</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
cols = set(logWithIPEnhancedDF.columns)

dfTest("ET2-P-05-02-01", True, "EnglishShortName" in cols and "ip" in cols)
dfTest("ET2-P-05-02-02", True, "alpha2Code" not in cols and "ISO31662SubdivisionCode" not in cols)
dfTest("ET2-P-05-02-03", 5000, logWithIPEnhancedDF.count())

print("Tests passed!")
```

    Tests passed!


## Review
**Question:** Why are joins expensive operations?  
**Answer:** Joins perform a large number of row-wise comparisons, making the cost associated with joining tables grow with the size of the data in the tables.

**Question:** What is the difference between a shuffle and broadcast join? How does Spark manage these differences?  
**Answer:** A shuffle join shuffles data between nodes in a cluster. By contrast, a broadcast join moves the smaller of two DataFrames to where the larger DataFrame sits, minimizing the overall data transfer. By default, Spark performs a broadcast join if the total number of records is below a certain threshold. The threshold can be manually specified or you can manually specify that a broadcast join should take place. Since the automatic determination of whether a shuffle join should take place is by number of records, this could mean that really wide data would take up significantly more space per record and should therefore be specified manually.

**Question:** What is a lookup table?  
**Answer:** A lookup table is small table often used for referencing commonly used data such as mapping cities to countries.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
