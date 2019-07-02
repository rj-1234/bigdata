
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Database Writes

Apache Spark&trade; allows you to write to a number of target databases in parallel, storing the transformed data from from your ETL job.

## In this lesson you:
* Write to a target database in serial and in parallel
* Repartition DataFrames to optimize table inserts
* Coalesce DataFrames to minimize data shuffling


### Database Writes in Spark

Writing to a database in Spark differs from other tools largely due to its distributed nature. There are a number of variables that can be tweaked to optimize performance, largely relating to how data is organized on the cluster. Partitions are the first step in understanding performant database connections.

**A partition is a portion of your total data set,** which is divided into many of these portions so Spark can distribute your work across a cluster. 

The other concept needed to understand Spark's computation is a slot (also known as a core). **A slot/core is a resource available for the execution of computation in parallel.** In brief, a partition refers to the distribution of data while a slot refers to the distribution of computation.

<div><img src="../../resources/partitions-and-cores.png" style="height: 400px; margin: 20px"/></div>

As a general rule of thumb, the number of partitions should be a multiple of the number of cores. For instance, with 5 partitions and 8 slots, 3 of the slots will be underutilized. With 9 partitions and 8 slots, a job will take twice as long as it waits for the extra partition to finish.

### Managing Partitions

In the context of JDBC database writes, **the number of partitions determine the number of connections used to push data through the JDBC API.** There are two ways to control this parallelism:  

| Function | Transformation Type | Use | Evenly distributes data across partitions? |
| :----------------|:----------------|:----------------|:----------------| 
| `.coalesce(n)`   | narrow (does not shuffle data) | reduce the number of partitions | no |
| `.repartition(n)`| wide (includes a shuffle operation) | increase the number of partitions | yes |

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
            setAppName("pyspark_etl_10-database-writes").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_10-database-writes").\
            setMaster('yarn-client').\
            set('spark.executor.cores', SPARK_EXECUTOR_CORE).\
            set('spark.executor.memory', SPARK_EXECUTOR_MEMORY).\
            set('spark.driver.cores', SPARK_DRIVER_CORE).\
            set('spark.driver.memory', SPARK_DRIVER_MEMORY).\
            set("spark.executor.instances", SPARK_EXECUTOR_INSTANCES).\
            set('spark.sql.files.ignoreCorruptFiles', 'true').\
            set('spark.yarn.dist.archives', '../../libs/pyspark24_py36.zip#MN').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1'). \
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


Start by importing a DataFrame of Wikipedia pageviews.


```python
wikiDF = (spark.read
  .parquet("s3a://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second.parquet")
)
display(wikiDF)
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015-03-22T14:13:34</td>
      <td>mobile</td>
      <td>1425</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015-03-22T14:23:18</td>
      <td>desktop</td>
      <td>2534</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015-03-22T14:36:47</td>
      <td>desktop</td>
      <td>2444</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015-03-22T14:38:39</td>
      <td>mobile</td>
      <td>1488</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015-03-22T14:57:11</td>
      <td>mobile</td>
      <td>1519</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2015-03-22T15:03:18</td>
      <td>mobile</td>
      <td>1559</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2015-03-22T15:16:47</td>
      <td>mobile</td>
      <td>1510</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2015-03-22T15:45:03</td>
      <td>desktop</td>
      <td>2673</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2015-03-22T15:58:32</td>
      <td>desktop</td>
      <td>2463</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2015-03-22T16:06:11</td>
      <td>desktop</td>
      <td>2525</td>
    </tr>
  </tbody>
</table>
</div>



View the number of partitions by changing the DataFrame into an RDD and using the `.getNumPartitions()` method.  
Since the Parquet file was saved with 5 partitions, those partitions are retained when you import the data.


```python
partitions = wikiDF.rdd.getNumPartitions()
print("Partitions: {0:,}".format( partitions ))
```

    Partitions: 5


To increase the number of partitions to 16, use `.repartition()`.


```python
repartitionedWikiDF = wikiDF.repartition(16)
print("Partitions: {0:,}".format( repartitionedWikiDF.rdd.getNumPartitions() ))
```

    Partitions: 16


To reduce the number of partitions, use `.coalesce()`.


```python
coalescedWikiDF = repartitionedWikiDF.coalesce(2)
print("Partitions: {0:,}".format( coalescedWikiDF.rdd.getNumPartitions() ))
```

    Partitions: 2


### Configure Default Partitions

Spark uses a default value of 200 partitions, which comes from real-world experience by Spark engineers. This is an adjustable configuration setting. Run the following cell to see this value.

Get and set any number of different configuration settings in this manner. <a href="https://spark.apache.org/docs/latest/configuration.html" target="_blank">See the Spark documents</a> for details.


```python
spark.conf.get("spark.sql.shuffle.partitions")
```




    '200'



Adjust the number of partitions with the following cell.  **This changes the number of partitions after a shuffle operation.**


```python
spark.conf.set("spark.sql.shuffle.partitions", "8")
```

Now check to see how this changes an operation involving a data shuffle, such as an `.orderBy()`.  Recall that coalesced `coalescedWikiDF` to 2 partitions.


```python
orderByPartitions = coalescedWikiDF.orderBy("requests").rdd.getNumPartitions()
print("Partitions: {0:,}".format( orderByPartitions ))
```

    Partitions: 8


The `.orderBy()` triggered the repartition of the DataFrame into 8 partitions.  Now reset the default value.


```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Parallel Database Writes

In that lesson you defined the number of partitions in the call to the database.  

**By contrast and when writing to a database, the number of active connections to the database is determined by the number of partitions of the DataFrame.**

Examine this by writing `wikiDF` to the `/tmp` directory.  Recall that `wikiDF` has 5 partitions.


```python
YOUR_FIRST_NAME = "rajeev" # CHANGE ME!
userhome = f"s3a://temp.intellinum.co/{YOUR_FIRST_NAME}"
```


```python
wikiDF.write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")
```

Examine the number of partitions in `wiki.parquet`.


```python
!aws s3 ls {userhome.replace("s3a","s3")+"/wiki.parquet/"}
```

    2019-06-24 18:46:33          0 _SUCCESS
    2019-06-24 18:46:32   25172128 part-00000-1ebb3dfb-9593-4dff-ae43-d00839f879d7-c000.snappy.parquet
    2019-06-24 18:46:30   24794655 part-00001-1ebb3dfb-9593-4dff-ae43-d00839f879d7-c000.snappy.parquet
    2019-06-24 18:46:29   11632341 part-00002-1ebb3dfb-9593-4dff-ae43-d00839f879d7-c000.snappy.parquet


This file has 5 parts, meaning Spark wrote the data through 5 different connections to this directory in the file system.

### Examining in the Spark UI

Click the hyperlink to `Spark UI` under the following code cell in order to see a breakdown of the job you triggered. The link might not work as is, you need to replace the `ip-xx-xx-xx-xx.ec2.internal` part with the DNS url of the jupyter. For example:

---
Change
```
http://ip-172-31-20-26.ec2.internal:4043/
```

to

```
http://ec2-54-172-218-250.compute-1.amazonaws.com:4043/
```
---

You will be redirected to port `20888`. Again , the link doesn't work, you need to fix the `ip-xx-xx-xx-xx.ec2.internal` part. For example:

Change
```
http://ip-172-31-20-26.ec2.internal:20888/proxy/application_1561313424801_0011/
```

to

```

http://ec2-54-172-218-250.compute-1.amazonaws.com:20888/proxy/application_1561313424801_0011/
```


```python
spark
```





            <div>
                <p><b>SparkSession - in-memory</b></p>
                
        <div>
            <p><b>SparkContext</b></p>

            <p><a href="http://ip-172-31-20-26.ec2.internal:4044">Spark UI</a></p>

            <dl>
              <dt>Version</dt>
                <dd><code>v2.4.2</code></dd>
              <dt>Master</dt>
                <dd><code>yarn-client</code></dd>
              <dt>AppName</dt>
                <dd><code>pyspark_etl_10-database-writes</code></dd>
            </dl>
        </div>
        
            </div>
        



Take a look at the jobs tab on Spark UI while you run the following cells


```python
wikiDF.repartition(12).write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")
```

5 stages were initially triggered, one for each partition of our data.  When you repartitioned the DataFrame to 12 partitions, 12 stages were needed, one to write each partition of the data.  Run the following and observe how the repartitioning changes the number of stages.


```python
wikiDF.repartition(10).write.mode("OVERWRITE").parquet(userhome+"/wiki.parquet")
```

### A Note on Upserts

Upserts insert a record into a database if it doesn't already exist, and updates the existing record if it does.  **Upserts are not supported in core Spark** due to the transactional nature of upserting and the immutable nature of Spark. You can only append or overwrite.  Databricks offers a data management system called Databricks Delta that does allow for upserts and other transactional functionality. [See the Databricks Delta docs for more information.](https://docs.databricks.com/delta/delta-batch.html#upserts-merge-into)

## Exercise 1: Changing Partitions

Change the number of partitions to prepare the optimal database write.

### Step 1: Import Helper Functions and Data

A function is defined for you to print out the number of records in each DataFrame.  Run the following cell to define that function.


```python
def printRecordsPerPartition(df):
    '''
    Utility method to count & print the number of records in each partition
    '''
    print("Per-Partition Counts:")
    
    def countInPartition(iterator): 
        yield __builtin__.sum(1 for _ in iterator)
      
    results = (df.rdd                   # Convert to an RDD
      .mapPartitions(countInPartition)  # For each partition, count
      .collect()                        # Return the counts to the driver
    )

    for result in results: 
        print("* " + str(result))
```

### Import the data to sitting in
`s3a://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second.parquet` to `wikiDF`.



```python
# TODO
path = 's3a://data.intellinum.co/bootcamp/common/wikipedia/pageviews/pageviews_by_second.parquet'
wikiDF = spark.read.parquet(path)

display(wikiDF)
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2015-03-22T14:13:34</td>
      <td>mobile</td>
      <td>1425</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2015-03-22T14:23:18</td>
      <td>desktop</td>
      <td>2534</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2015-03-22T14:36:47</td>
      <td>desktop</td>
      <td>2444</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2015-03-22T14:38:39</td>
      <td>mobile</td>
      <td>1488</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2015-03-22T14:57:11</td>
      <td>mobile</td>
      <td>1519</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2015-03-22T15:03:18</td>
      <td>mobile</td>
      <td>1559</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2015-03-22T15:16:47</td>
      <td>mobile</td>
      <td>1510</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2015-03-22T15:45:03</td>
      <td>desktop</td>
      <td>2673</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2015-03-22T15:58:32</td>
      <td>desktop</td>
      <td>2463</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2015-03-22T16:06:11</td>
      <td>desktop</td>
      <td>2525</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
dfTest("ET2-P-06-01-01", 7200000, wikiDF.count())
dfTest("ET2-P-06-01-02", ['timestamp', 'site', 'requests'], wikiDF.columns)

print("Tests passed!")
```

    Tests passed!


Print the count of records by partition using `printRecordsPerPartition()`.


```python
printRecordsPerPartition(wikiDF)
```

    Per-Partition Counts:
    * 2926025
    * 2926072
    * 1347903


### Step 2: Repartition the Data

Define three new DataFrames:

* `wikiDF1Partition`: `wikiDF` with 1 partition
* `wikiDF16Partition`: `wikiDF` with 16 partitions
* `wikiDF128Partition`: `wikiDF` with 128 partitions


```python
# TODO
wikiDF1Partition = wikiDF.repartition(1)
wikiDF16Partition = wikiDF.repartition(16)
wikiDF128Partition = wikiDF.repartition(128)

print(wikiDF1Partition.rdd.getNumPartitions())
print(wikiDF16Partition.rdd.getNumPartitions())
print(wikiDF128Partition.rdd.getNumPartitions())
```

    1
    16
    128



```python
# TEST - Run this cell to test your solution

dfTest("ET2-P-06-02-01", 1, wikiDF1Partition.rdd.getNumPartitions())
dfTest("ET2-P-06-02-02", 16, wikiDF16Partition.rdd.getNumPartitions())
dfTest("ET2-P-06-02-03", 128, wikiDF128Partition.rdd.getNumPartitions())

print("Tests passed!")
```

    Tests passed!


### Step 3: Examine the Distribution of Records

Use `printRecordsPerPartition()` to examine the distribution of records across the partitions.


```python
printRecordsPerPartition(wikiDF1Partition)
printRecordsPerPartition(wikiDF16Partition)
printRecordsPerPartition(wikiDF128Partition)
```

    Per-Partition Counts:
    * 7200000
    Per-Partition Counts:
    * 450001
    * 450001
    * 450001
    * 450001
    * 450000
    * 449999
    * 449999
    * 449999
    * 449999
    * 449999
    * 449999
    * 449998
    * 450001
    * 450001
    * 450001
    * 450001
    Per-Partition Counts:
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56250
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56249
    * 56248
    * 56248
    * 56248
    * 56248
    * 56248
    * 56248
    * 56248
    * 56248
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251
    * 56251


### Step 4: Coalesce `wikiDF16Partition` and Examine the Results

Coalesce `wikiDF16Partition` to `10` partitions, saving the result to `wikiDF16PartitionCoalesced`.


```python
wikiDF16PartitionCoalesced = wikiDF16Partition.coalesce(10)

print(wikiDF16PartitionCoalesced.rdd.getNumPartitions())
```

    10



```python
# TEST - Run this cell to test your solution

dfTest("ET2-P-06-03-01", 10, wikiDF16PartitionCoalesced.rdd.getNumPartitions())

print("Tests passed!")
```

    Tests passed!


Examine the new distribution of data using `printRecordsPerPartition`.  Is the distribution uniform?  Why or why not?

## Review
**Question:** How do you determine the number of connections to the database you write to?  
**Answer:** Spark makes one connection for each partition in your data. Increasing the number of partitions increases the database connections.

**Question:** How do you increase and decrease the number of partitions in your data?  
**Answer:** `.repartitions(n)` increases the number of partitions in your data. It can also decrease the number of partitions, but since this is a wide operation it should be used sparingly.  `.coalesce(n)` decreases the number of partitions in your data. If you use `.coalesce(n)` with a number greater than the current partitions, this DataFrame method will have no effect.

**Question:** How can you change the default number of partitions?  
**Answer:** Changing the configuration parameter `spark.sql.shuffle.partitions` will alter the default number of partitions.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
