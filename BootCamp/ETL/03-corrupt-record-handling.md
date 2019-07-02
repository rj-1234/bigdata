
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Corrupt Record Handling

Apache Spark&trade; provide ways to handle corrupt records.

## In this lesson you:
* Define corruption logic to handle corrupt records
* Pipe corrupt records into a directory for later analysis

## Working with Corrupt Data

ETL pipelines need robust solutions to handle corrupt data. This is because data corruption scales as the size of data and complexity of the data application grow. Corrupt data includes:  
<br>
* Missing information
* Incomplete information
* Schema mismatch
* Differing formats or data types
* User errors when writing data producers

Since ETL pipelines are built to be automated, production-oriented solutions must ensure pipelines behave as expected. This means that **data engineers must both expect and systematically handle corrupt records.**

In the road map for ETL, this is the **Handle Corrupt Records** step:
<img src="../../resources/ETL-Process-3.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>


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
            setAppName("pyspark_etl_03-corrupt-record-handling").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_03-corrupt-record-handling").\
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


Run the following cell, which contains a corrupt record, `{"a": 1, "b, "c":10}`:

This is not the preferred way to make a DataFrame.  This code allows us to mimic a corrupt record you might see in production.


```python
data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "PERMISSIVE")
  .option("columnNameOfCorruptRecord", "_corrupt_record")
  .json(sc.parallelize(data))
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
      <th>_corrupt_record</th>
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{"a": 1, "b, "c":10}</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



In the previous results, Spark parsed the corrupt record into its own column and processed the other records as expected. This is the default behavior for corrupt records, so you didn't technically need to use the two options `mode` and `columnNameOfCorruptRecord`.

There are three different options for handling corrupt records [set through the `ParseMode` option](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/ParseMode.scala#L34):

| `ParseMode` | Behavior |
|-------------|----------|
| `PERMISSIVE` | Includes corrupt records in a "_corrupt_record" column (by default) |
| `DROPMALFORMED` | Ignores all corrupted records |
| `FAILFAST` | Throws an exception when it meets corrupted records |

The following cell acts on the same data but drops corrupt records:


```python
data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("mode", "DROPMALFORMED")
  .json(sc.parallelize(data))
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
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2</td>
      <td>3</td>
    </tr>
  </tbody>
</table>
</div>



The following cell throws an error once a corrupt record is found, rather than ignoring or saving the corrupt records:


```python
try:
    data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

    corruptDF = (spark.read
      .option("mode", "FAILFAST")
      .json(sc.parallelize(data))
    )
    display(corruptDF)
    
except Exception as e:
    print(e)
```

    An error occurred while calling o191.json.
    : org.apache.spark.SparkException: Job aborted due to stage failure: Task 4999 in stage 16.0 failed 4 times, most recent failure: Lost task 4999.3 in stage 16.0 (TID 25002, ip-172-31-47-2.us-east-2.compute.internal, executor 7): org.apache.spark.SparkException: Malformed records are detected in schema inference. Parse Mode: FAILFAST.
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:66)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:53)
    	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:435)
    	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:441)
    	at scala.collection.Iterator$class.isEmpty(Iterator.scala:331)
    	at scala.collection.AbstractIterator.isEmpty(Iterator.scala:1334)
    	at scala.collection.TraversableOnce$class.reduceLeftOption(TraversableOnce.scala:203)
    	at scala.collection.AbstractIterator.reduceLeftOption(Iterator.scala:1334)
    	at scala.collection.TraversableOnce$class.reduceOption(TraversableOnce.scala:210)
    	at scala.collection.AbstractIterator.reduceOption(Iterator.scala:1334)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1.apply(JsonInferSchema.scala:70)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1.apply(JsonInferSchema.scala:50)
    	at org.apache.spark.rdd.RDD$$anonfun$mapPartitions$1$$anonfun$apply$23.apply(RDD.scala:801)
    	at org.apache.spark.rdd.RDD$$anonfun$mapPartitions$1$$anonfun$apply$23.apply(RDD.scala:801)
    	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
    	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:324)
    	at org.apache.spark.rdd.RDD.iterator(RDD.scala:288)
    	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
    	at org.apache.spark.scheduler.Task.run(Task.scala:121)
    	at org.apache.spark.executor.Executor$TaskRunner$$anonfun$10.apply(Executor.scala:402)
    	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1360)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:408)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	at java.lang.Thread.run(Thread.java:748)
    Caused by: com.fasterxml.jackson.core.JsonParseException: Unexpected character ('c' (code 99)): was expecting a colon to separate field name and value
     at [Source: [B@1259b8ca; line: 1, column: 16]
    	at com.fasterxml.jackson.core.JsonParser._constructError(JsonParser.java:1581)
    	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportError(ParserMinimalBase.java:533)
    	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportUnexpectedChar(ParserMinimalBase.java:462)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser._skipColon2(UTF8StreamJsonParser.java:3009)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser._skipColon(UTF8StreamJsonParser.java:2984)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser.nextToken(UTF8StreamJsonParser.java:744)
    	at org.apache.spark.sql.catalyst.json.JacksonUtils$.nextUntil(JacksonUtils.scala:29)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$.inferField(JsonInferSchema.scala:134)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1$$anonfun$apply$3.apply(JsonInferSchema.scala:57)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1$$anonfun$apply$3.apply(JsonInferSchema.scala:55)
    	at org.apache.spark.util.Utils$.tryWithResource(Utils.scala:2543)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:55)
    	... 24 more
    
    Driver stacktrace:
    	at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:2039)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:2027)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:2026)
    	at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
    	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
    	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2026)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:966)
    	at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:966)
    	at scala.Option.foreach(Option.scala:257)
    	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:966)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2260)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2209)
    	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2198)
    	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
    	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:777)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2061)
    	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2158)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$.infer(JsonInferSchema.scala:83)
    	at org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource$$anonfun$inferFromDataset$1.apply(JsonDataSource.scala:109)
    	at org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource$$anonfun$inferFromDataset$1.apply(JsonDataSource.scala:109)
    	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:125)
    	at org.apache.spark.sql.execution.datasources.json.TextInputJsonDataSource$.inferFromDataset(JsonDataSource.scala:108)
    	at org.apache.spark.sql.DataFrameReader$$anonfun$2.apply(DataFrameReader.scala:439)
    	at org.apache.spark.sql.DataFrameReader$$anonfun$2.apply(DataFrameReader.scala:439)
    	at scala.Option.getOrElse(Option.scala:121)
    	at org.apache.spark.sql.DataFrameReader.json(DataFrameReader.scala:438)
    	at org.apache.spark.sql.DataFrameReader.json(DataFrameReader.scala:419)
    	at org.apache.spark.sql.DataFrameReader.json(DataFrameReader.scala:405)
    	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
    	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
    	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
    	at java.lang.reflect.Method.invoke(Method.java:498)
    	at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
    	at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
    	at py4j.Gateway.invoke(Gateway.java:282)
    	at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
    	at py4j.commands.CallCommand.execute(CallCommand.java:79)
    	at py4j.GatewayConnection.run(GatewayConnection.java:238)
    	at java.lang.Thread.run(Thread.java:748)
    Caused by: org.apache.spark.SparkException: Malformed records are detected in schema inference. Parse Mode: FAILFAST.
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:66)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:53)
    	at scala.collection.Iterator$$anon$12.nextCur(Iterator.scala:435)
    	at scala.collection.Iterator$$anon$12.hasNext(Iterator.scala:441)
    	at scala.collection.Iterator$class.isEmpty(Iterator.scala:331)
    	at scala.collection.AbstractIterator.isEmpty(Iterator.scala:1334)
    	at scala.collection.TraversableOnce$class.reduceLeftOption(TraversableOnce.scala:203)
    	at scala.collection.AbstractIterator.reduceLeftOption(Iterator.scala:1334)
    	at scala.collection.TraversableOnce$class.reduceOption(TraversableOnce.scala:210)
    	at scala.collection.AbstractIterator.reduceOption(Iterator.scala:1334)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1.apply(JsonInferSchema.scala:70)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1.apply(JsonInferSchema.scala:50)
    	at org.apache.spark.rdd.RDD$$anonfun$mapPartitions$1$$anonfun$apply$23.apply(RDD.scala:801)
    	at org.apache.spark.rdd.RDD$$anonfun$mapPartitions$1$$anonfun$apply$23.apply(RDD.scala:801)
    	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
    	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:324)
    	at org.apache.spark.rdd.RDD.iterator(RDD.scala:288)
    	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
    	at org.apache.spark.scheduler.Task.run(Task.scala:121)
    	at org.apache.spark.executor.Executor$TaskRunner$$anonfun$10.apply(Executor.scala:402)
    	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1360)
    	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:408)
    	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
    	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
    	... 1 more
    Caused by: com.fasterxml.jackson.core.JsonParseException: Unexpected character ('c' (code 99)): was expecting a colon to separate field name and value
     at [Source: UNKNOWN; line: 1, column: 16]
    	at com.fasterxml.jackson.core.JsonParser._constructError(JsonParser.java:1581)
    	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportError(ParserMinimalBase.java:533)
    	at com.fasterxml.jackson.core.base.ParserMinimalBase._reportUnexpectedChar(ParserMinimalBase.java:462)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser._skipColon2(UTF8StreamJsonParser.java:3009)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser._skipColon(UTF8StreamJsonParser.java:2984)
    	at com.fasterxml.jackson.core.json.UTF8StreamJsonParser.nextToken(UTF8StreamJsonParser.java:744)
    	at org.apache.spark.sql.catalyst.json.JacksonUtils$.nextUntil(JacksonUtils.scala:29)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$.inferField(JsonInferSchema.scala:134)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1$$anonfun$apply$3.apply(JsonInferSchema.scala:57)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1$$anonfun$apply$3.apply(JsonInferSchema.scala:55)
    	at org.apache.spark.util.Utils$.tryWithResource(Utils.scala:2543)
    	at org.apache.spark.sql.catalyst.json.JsonInferSchema$$anonfun$1$$anonfun$apply$1.apply(JsonInferSchema.scala:55)
    	... 24 more
    


### Recommended Pattern: `badRecordsPath`

Databricks Runtime has [a built-in feature](https://docs.databricks.com/spark/latest/spark-sql/handling-bad-records.html) that saves corrupt records to a given end point. To use this, set the `badRecordsPath`.

This is a preferred design pattern since it persists the corrupt records for later analysis even after the cluster shuts down.


```python
#TODO
YOUR_FIRST_NAME = "rajeev"
```


```python
basePath = "s3://temp.intellinum.co/{}/etl1p".format(YOUR_FIRST_NAME)
myBadRecords = "{}/badRecordsPath".format(basePath)

print("""Your temp directory is "{}" """.format(myBadRecords))
```

    Your temp directory is "s3://temp.intellinum.co/rajeev/etl1p/badRecordsPath" 



```python
!aws s3 ls temp.intellinum.co/rajeev/
```

                               PRE ipCount.parquet/
                               PRE serverErrorDF.parquet/



```python
data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')

corruptDF = (spark.read
  .option("badRecordsPath", myBadRecords)
  .json(sc.parallelize(data))
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
      <th>_corrupt_record</th>
      <th>a</th>
      <th>b</th>
      <th>c</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>{"a": 1, "b, "c":10}</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



See the results in the path specified by `myBadRecords`.

Recall that this directory is backed by S3 and is available to all clusters.

Note: **Only Databricks runtime 3.0 supports badRecordsPath for now**


```python
path = "{}/*/*/*".format(myBadRecords)
display(spark.read.json(path))
```

## Exercise 1: Working with Corrupt Records

### Step 1: Diagnose the Problem

Import the data used in the last lesson, which is located at `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*`.  Import the corrupt records in a new column `SMSCorrupt`.  <br>

Save only the columns `SMS` and `SMSCorrupt` to the new DataFrame `SMSCorruptDF`.


```python
# TODO
from pyspark.sql.types import StructType, StructField, StringType

path = "s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*"
schema = StructType([
    StructField('SMS', StringType(), True),
    StructField('_corrupt_record', StringType(), True)
])
SMSDF = spark.read.schema(schema).json(path).filter('_corrupt_record != "None" ')
```


```python
display(SMSDF)
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
      <th>SMS</th>
      <th>_corrupt_record</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>None</td>
      <td>{"SMS":{"Address":"+98912503####","type":"1","...</td>
    </tr>
  </tbody>
</table>
</div>




```python
SMSCorruptDF = SMSDF.select(F.col('SMS'), F.col('_corrupt_record').alias('SMSCorrupt'))
SMSCorruptDF.printSchema()
```

    root
     |-- SMS: string (nullable = true)
     |-- SMSCorrupt: string (nullable = true)
    



```python
# TEST - Run this cell to test your solution
cols = set(SMSCorruptDF.columns)
SMSCount = SMSCorruptDF.cache().count()

dfTest("ET1-P-06-01-01", True, "SMS" in cols)
dfTest("ET1-P-06-01-02", True, "SMSCorrupt" in cols)
dfTest("ET1-P-06-01-03", 8, SMSCount)

print("Tests passed!")
```

    Tests passed!


Examine the corrupt records to determine what the problem is with the bad records.

**Hint:** Take a look at the name in metadata.

The entry `{"name": "mr Khojasteh"flash""}` should have single quotes around `flash` since the double quotes are interpreted as the end of the value.  It should read `{"name": "mr Khojasteh'flash'"}` instead.

The optimal solution is to fix the initial producer of the data to correct the problem at its source.  In the meantime, you could write ad hoc logic to turn this into a readable field.

### Step 2: Use `badRecordsPath`

Use the `badRecordsPath` option to save corrupt records to the directory specified by the `corruptPath` variable below.


```python
# TODO
data = SMSCorruptDF.select(F.col('SMSCorrupt'))
SMSCorruptDF2 = spark.read.option('badRecordsPath', myBadRecords).json(sc.parallelize(data))
```


```python
# TEST - Run this cell to test your solution
SMSCorruptDF2.count()

testPath = "{}/corruptSMS/*/*/*".format(basePath)
corruptCount = spark.read.json(testPath).count()

dfTest("ET1-P-06-02-01", True, corruptCount >= 8)

print("Tests passed!")
```

One last step... let's clean up our temp files:


```python
!aws s3 rm --recursive {basePath}
```

    
    usage: aws s3 rm <S3Uri>
    Error: Invalid argument type


## Review
**Question:** By default, how are corrupt records dealt with using `spark.read.json()`?  
**Answer:** They appear in a column called `_corrupt_record`.

**Question:** How can a query persist corrupt records in separate destination?  
**Answer:** The Databricks runtime 3.0 supports a feature called `badRecordsPath` that allows a query to save corrupt records to a given end point for the pipeline engineer to investigate corruption issues. But this feature is not available in Opensourced version of the Spark yet.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
