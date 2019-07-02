
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Capstone Project: Parsing Nested Data

Mount JSON data using S3, define and apply a schema, parse fields, and save the cleaned results back to S3.


## Instructions

A common source of data in ETL pipelines is <a href="https://kafka.apache.org/" target="_blank">Apache Kafka</a>, or the managed alternative
<a href="https://aws.amazon.com/kinesis/" target="_blank">Kinesis</a>.
A common data type in these use cases is newline-separated JSON.

For this exercise, Tweets were streamed from the <a href="https://developer.twitter.com/en/docs" target="_blank">Twitter firehose API</a> into such an aggregation server and,
from there, dumped into the distributed file system.

Use these four exercises to perform ETL on the data in this bucket:  
<br>
1. Extracting and Exploring the Data
2. Defining and Applying a Schema
3. Creating the Tables
4. Loading the Results

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
            setAppName("pyspark_etl_05-parsing-nested-data-project").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_05-parsing-nested-data-project-rajeev").\
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


## Exercise 1: Extracting and Exploring the Data

First, review the data. 

### Step 1: Explore the Folder Structure

Explore the mount and review the directory structure. Use `%fs ls`.  The data is located in `s3://data.intellinum.co/bootcamp/common/twitter/firehose/`


```python
# TODO (Use AWS CLI)
!aws s3 ls data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/
```

    2019-06-12 19:04:48    7945820 twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4
    2019-06-12 19:04:48   12527115 twitterstream-1-2018-01-08-18-58-00-90ebdcae-ee96-443d-bd8b-de09ece454c2


### Step 2: Explore a Single File

> "Premature optimization is the root of all evil." -Sir Tony Hoare

There are a few gigabytes of Twitter data available in the directory. Hoare's law about premature optimization is applicable here.  Instead of building a schema for the entire data set and then trying it out, an iterative process is much less error prone and runs much faster. Start by working on a single file before you apply your proof of concept across the entire data set.

Read a single file.  Start with `twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4`. Find this in `s3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/`.  Save the results to the variable `df`.


```python
# TODO
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'
df = spark.read.json(path)
```


```python
# TEST - Run this cell to test your solution
cols = df.columns

dfTest("ET1-P-08-02-01", 1744, df.count())
dfTest("ET1-P-08-02-02", True, "id" in cols)
dfTest("ET1-P-08-02-03", True, "text" in cols)

print("Tests passed!")
```

    Tests passed!


Display the schema.


```python
# TODO
df.printSchema()
```

    root
     |-- contributors: string (nullable = true)
     |-- coordinates: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- created_at: string (nullable = true)
     |-- delete: struct (nullable = true)
     |    |-- status: struct (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- user_id: long (nullable = true)
     |    |    |-- user_id_str: string (nullable = true)
     |    |-- timestamp_ms: string (nullable = true)
     |-- display_text_range: array (nullable = true)
     |    |-- element: long (containsNull = true)
     |-- entities: struct (nullable = true)
     |    |-- hashtags: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- description: string (nullable = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- symbols: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- urls: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- url: string (nullable = true)
     |    |-- user_mentions: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- screen_name: string (nullable = true)
     |-- extended_entities: struct (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- description: string (nullable = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |-- url: string (nullable = true)
     |-- extended_tweet: struct (nullable = true)
     |    |-- display_text_range: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- full_text: string (nullable = true)
     |-- favorite_count: long (nullable = true)
     |-- favorited: boolean (nullable = true)
     |-- filter_level: string (nullable = true)
     |-- geo: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- hangup: boolean (nullable = true)
     |-- heartbeat_timeout: boolean (nullable = true)
     |-- id: long (nullable = true)
     |-- id_str: string (nullable = true)
     |-- in_reply_to_screen_name: string (nullable = true)
     |-- in_reply_to_status_id: long (nullable = true)
     |-- in_reply_to_status_id_str: string (nullable = true)
     |-- in_reply_to_user_id: long (nullable = true)
     |-- in_reply_to_user_id_str: string (nullable = true)
     |-- is_quote_status: boolean (nullable = true)
     |-- lang: string (nullable = true)
     |-- place: struct (nullable = true)
     |    |-- bounding_box: struct (nullable = true)
     |    |    |-- coordinates: array (nullable = true)
     |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |-- type: string (nullable = true)
     |    |-- country: string (nullable = true)
     |    |-- country_code: string (nullable = true)
     |    |-- full_name: string (nullable = true)
     |    |-- id: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- place_type: string (nullable = true)
     |    |-- url: string (nullable = true)
     |-- possibly_sensitive: boolean (nullable = true)
     |-- quote_count: long (nullable = true)
     |-- quoted_status: struct (nullable = true)
     |    |-- contributors: string (nullable = true)
     |    |-- coordinates: string (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- display_text_range: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- extended_tweet: struct (nullable = true)
     |    |    |-- display_text_range: array (nullable = true)
     |    |    |    |-- element: long (containsNull = true)
     |    |    |-- entities: struct (nullable = true)
     |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |-- extended_entities: struct (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- full_text: string (nullable = true)
     |    |-- favorite_count: long (nullable = true)
     |    |-- favorited: boolean (nullable = true)
     |    |-- filter_level: string (nullable = true)
     |    |-- geo: string (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- in_reply_to_screen_name: string (nullable = true)
     |    |-- in_reply_to_status_id: long (nullable = true)
     |    |-- in_reply_to_status_id_str: string (nullable = true)
     |    |-- in_reply_to_user_id: long (nullable = true)
     |    |-- in_reply_to_user_id_str: string (nullable = true)
     |    |-- is_quote_status: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- place: struct (nullable = true)
     |    |    |-- bounding_box: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- country: string (nullable = true)
     |    |    |-- country_code: string (nullable = true)
     |    |    |-- full_name: string (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- place_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |-- possibly_sensitive: boolean (nullable = true)
     |    |-- quote_count: long (nullable = true)
     |    |-- quoted_status_id: long (nullable = true)
     |    |-- quoted_status_id_str: string (nullable = true)
     |    |-- reply_count: long (nullable = true)
     |    |-- retweet_count: long (nullable = true)
     |    |-- retweeted: boolean (nullable = true)
     |    |-- source: string (nullable = true)
     |    |-- text: string (nullable = true)
     |    |-- truncated: boolean (nullable = true)
     |    |-- user: struct (nullable = true)
     |    |    |-- contributors_enabled: boolean (nullable = true)
     |    |    |-- created_at: string (nullable = true)
     |    |    |-- default_profile: boolean (nullable = true)
     |    |    |-- default_profile_image: boolean (nullable = true)
     |    |    |-- description: string (nullable = true)
     |    |    |-- favourites_count: long (nullable = true)
     |    |    |-- follow_request_sent: string (nullable = true)
     |    |    |-- followers_count: long (nullable = true)
     |    |    |-- following: string (nullable = true)
     |    |    |-- friends_count: long (nullable = true)
     |    |    |-- geo_enabled: boolean (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- is_translator: boolean (nullable = true)
     |    |    |-- lang: string (nullable = true)
     |    |    |-- listed_count: long (nullable = true)
     |    |    |-- location: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- notifications: string (nullable = true)
     |    |    |-- profile_background_color: string (nullable = true)
     |    |    |-- profile_background_image_url: string (nullable = true)
     |    |    |-- profile_background_image_url_https: string (nullable = true)
     |    |    |-- profile_background_tile: boolean (nullable = true)
     |    |    |-- profile_banner_url: string (nullable = true)
     |    |    |-- profile_image_url: string (nullable = true)
     |    |    |-- profile_image_url_https: string (nullable = true)
     |    |    |-- profile_link_color: string (nullable = true)
     |    |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |    |-- profile_text_color: string (nullable = true)
     |    |    |-- profile_use_background_image: boolean (nullable = true)
     |    |    |-- protected: boolean (nullable = true)
     |    |    |-- screen_name: string (nullable = true)
     |    |    |-- statuses_count: long (nullable = true)
     |    |    |-- time_zone: string (nullable = true)
     |    |    |-- translator_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |    |-- utc_offset: long (nullable = true)
     |    |    |-- verified: boolean (nullable = true)
     |-- quoted_status_id: long (nullable = true)
     |-- quoted_status_id_str: string (nullable = true)
     |-- reply_count: long (nullable = true)
     |-- retweet_count: long (nullable = true)
     |-- retweeted: boolean (nullable = true)
     |-- retweeted_status: struct (nullable = true)
     |    |-- contributors: string (nullable = true)
     |    |-- coordinates: string (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- display_text_range: array (nullable = true)
     |    |    |-- element: long (containsNull = true)
     |    |-- entities: struct (nullable = true)
     |    |    |-- hashtags: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- text: string (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- description: string (nullable = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- symbols: array (nullable = true)
     |    |    |    |-- element: string (containsNull = true)
     |    |    |-- urls: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |-- screen_name: string (nullable = true)
     |    |-- extended_entities: struct (nullable = true)
     |    |    |-- media: array (nullable = true)
     |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |-- description: string (nullable = true)
     |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |-- extended_tweet: struct (nullable = true)
     |    |    |-- display_text_range: array (nullable = true)
     |    |    |    |-- element: long (containsNull = true)
     |    |    |-- entities: struct (nullable = true)
     |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |-- extended_entities: struct (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- full_text: string (nullable = true)
     |    |-- favorite_count: long (nullable = true)
     |    |-- favorited: boolean (nullable = true)
     |    |-- filter_level: string (nullable = true)
     |    |-- geo: string (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- in_reply_to_screen_name: string (nullable = true)
     |    |-- in_reply_to_status_id: long (nullable = true)
     |    |-- in_reply_to_status_id_str: string (nullable = true)
     |    |-- in_reply_to_user_id: long (nullable = true)
     |    |-- in_reply_to_user_id_str: string (nullable = true)
     |    |-- is_quote_status: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- place: struct (nullable = true)
     |    |    |-- bounding_box: struct (nullable = true)
     |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |-- type: string (nullable = true)
     |    |    |-- country: string (nullable = true)
     |    |    |-- country_code: string (nullable = true)
     |    |    |-- full_name: string (nullable = true)
     |    |    |-- id: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- place_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |-- possibly_sensitive: boolean (nullable = true)
     |    |-- quote_count: long (nullable = true)
     |    |-- quoted_status: struct (nullable = true)
     |    |    |-- contributors: string (nullable = true)
     |    |    |-- coordinates: string (nullable = true)
     |    |    |-- created_at: string (nullable = true)
     |    |    |-- display_text_range: array (nullable = true)
     |    |    |    |-- element: long (containsNull = true)
     |    |    |-- entities: struct (nullable = true)
     |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |-- extended_entities: struct (nullable = true)
     |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |-- source_status_id: long (nullable = true)
     |    |    |    |    |    |-- source_status_id_str: string (nullable = true)
     |    |    |    |    |    |-- source_user_id: long (nullable = true)
     |    |    |    |    |    |-- source_user_id_str: string (nullable = true)
     |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |    |-- video_info: struct (nullable = true)
     |    |    |    |    |    |    |-- aspect_ratio: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- duration_millis: long (nullable = true)
     |    |    |    |    |    |    |-- variants: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |    |    |-- bitrate: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- content_type: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |-- extended_tweet: struct (nullable = true)
     |    |    |    |-- display_text_range: array (nullable = true)
     |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |-- entities: struct (nullable = true)
     |    |    |    |    |-- hashtags: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- text: string (nullable = true)
     |    |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- symbols: array (nullable = true)
     |    |    |    |    |    |-- element: string (containsNull = true)
     |    |    |    |    |-- urls: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |    |-- user_mentions: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- name: string (nullable = true)
     |    |    |    |    |    |    |-- screen_name: string (nullable = true)
     |    |    |    |-- extended_entities: struct (nullable = true)
     |    |    |    |    |-- media: array (nullable = true)
     |    |    |    |    |    |-- element: struct (containsNull = true)
     |    |    |    |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |    |    |    |-- id: long (nullable = true)
     |    |    |    |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |    |    |    |-- indices: array (nullable = true)
     |    |    |    |    |    |    |    |-- element: long (containsNull = true)
     |    |    |    |    |    |    |-- media_url: string (nullable = true)
     |    |    |    |    |    |    |-- media_url_https: string (nullable = true)
     |    |    |    |    |    |    |-- sizes: struct (nullable = true)
     |    |    |    |    |    |    |    |-- large: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- medium: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- small: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |    |-- thumb: struct (nullable = true)
     |    |    |    |    |    |    |    |    |-- h: long (nullable = true)
     |    |    |    |    |    |    |    |    |-- resize: string (nullable = true)
     |    |    |    |    |    |    |    |    |-- w: long (nullable = true)
     |    |    |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- full_text: string (nullable = true)
     |    |    |-- favorite_count: long (nullable = true)
     |    |    |-- favorited: boolean (nullable = true)
     |    |    |-- filter_level: string (nullable = true)
     |    |    |-- geo: string (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- in_reply_to_screen_name: string (nullable = true)
     |    |    |-- in_reply_to_status_id: long (nullable = true)
     |    |    |-- in_reply_to_status_id_str: string (nullable = true)
     |    |    |-- in_reply_to_user_id: long (nullable = true)
     |    |    |-- in_reply_to_user_id_str: string (nullable = true)
     |    |    |-- is_quote_status: boolean (nullable = true)
     |    |    |-- lang: string (nullable = true)
     |    |    |-- place: struct (nullable = true)
     |    |    |    |-- bounding_box: struct (nullable = true)
     |    |    |    |    |-- coordinates: array (nullable = true)
     |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |-- element: array (containsNull = true)
     |    |    |    |    |    |    |    |-- element: double (containsNull = true)
     |    |    |    |    |-- type: string (nullable = true)
     |    |    |    |-- country: string (nullable = true)
     |    |    |    |-- country_code: string (nullable = true)
     |    |    |    |-- full_name: string (nullable = true)
     |    |    |    |-- id: string (nullable = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- place_type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |    |-- possibly_sensitive: boolean (nullable = true)
     |    |    |-- quote_count: long (nullable = true)
     |    |    |-- quoted_status_id: long (nullable = true)
     |    |    |-- quoted_status_id_str: string (nullable = true)
     |    |    |-- reply_count: long (nullable = true)
     |    |    |-- retweet_count: long (nullable = true)
     |    |    |-- retweeted: boolean (nullable = true)
     |    |    |-- source: string (nullable = true)
     |    |    |-- text: string (nullable = true)
     |    |    |-- truncated: boolean (nullable = true)
     |    |    |-- user: struct (nullable = true)
     |    |    |    |-- contributors_enabled: boolean (nullable = true)
     |    |    |    |-- created_at: string (nullable = true)
     |    |    |    |-- default_profile: boolean (nullable = true)
     |    |    |    |-- default_profile_image: boolean (nullable = true)
     |    |    |    |-- description: string (nullable = true)
     |    |    |    |-- favourites_count: long (nullable = true)
     |    |    |    |-- follow_request_sent: string (nullable = true)
     |    |    |    |-- followers_count: long (nullable = true)
     |    |    |    |-- following: string (nullable = true)
     |    |    |    |-- friends_count: long (nullable = true)
     |    |    |    |-- geo_enabled: boolean (nullable = true)
     |    |    |    |-- id: long (nullable = true)
     |    |    |    |-- id_str: string (nullable = true)
     |    |    |    |-- is_translator: boolean (nullable = true)
     |    |    |    |-- lang: string (nullable = true)
     |    |    |    |-- listed_count: long (nullable = true)
     |    |    |    |-- location: string (nullable = true)
     |    |    |    |-- name: string (nullable = true)
     |    |    |    |-- notifications: string (nullable = true)
     |    |    |    |-- profile_background_color: string (nullable = true)
     |    |    |    |-- profile_background_image_url: string (nullable = true)
     |    |    |    |-- profile_background_image_url_https: string (nullable = true)
     |    |    |    |-- profile_background_tile: boolean (nullable = true)
     |    |    |    |-- profile_banner_url: string (nullable = true)
     |    |    |    |-- profile_image_url: string (nullable = true)
     |    |    |    |-- profile_image_url_https: string (nullable = true)
     |    |    |    |-- profile_link_color: string (nullable = true)
     |    |    |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |    |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |    |    |-- profile_text_color: string (nullable = true)
     |    |    |    |-- profile_use_background_image: boolean (nullable = true)
     |    |    |    |-- protected: boolean (nullable = true)
     |    |    |    |-- screen_name: string (nullable = true)
     |    |    |    |-- statuses_count: long (nullable = true)
     |    |    |    |-- time_zone: string (nullable = true)
     |    |    |    |-- translator_type: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |    |    |    |-- utc_offset: long (nullable = true)
     |    |    |    |-- verified: boolean (nullable = true)
     |    |-- quoted_status_id: long (nullable = true)
     |    |-- quoted_status_id_str: string (nullable = true)
     |    |-- reply_count: long (nullable = true)
     |    |-- retweet_count: long (nullable = true)
     |    |-- retweeted: boolean (nullable = true)
     |    |-- source: string (nullable = true)
     |    |-- text: string (nullable = true)
     |    |-- truncated: boolean (nullable = true)
     |    |-- user: struct (nullable = true)
     |    |    |-- contributors_enabled: boolean (nullable = true)
     |    |    |-- created_at: string (nullable = true)
     |    |    |-- default_profile: boolean (nullable = true)
     |    |    |-- default_profile_image: boolean (nullable = true)
     |    |    |-- description: string (nullable = true)
     |    |    |-- favourites_count: long (nullable = true)
     |    |    |-- follow_request_sent: string (nullable = true)
     |    |    |-- followers_count: long (nullable = true)
     |    |    |-- following: string (nullable = true)
     |    |    |-- friends_count: long (nullable = true)
     |    |    |-- geo_enabled: boolean (nullable = true)
     |    |    |-- id: long (nullable = true)
     |    |    |-- id_str: string (nullable = true)
     |    |    |-- is_translator: boolean (nullable = true)
     |    |    |-- lang: string (nullable = true)
     |    |    |-- listed_count: long (nullable = true)
     |    |    |-- location: string (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |    |-- notifications: string (nullable = true)
     |    |    |-- profile_background_color: string (nullable = true)
     |    |    |-- profile_background_image_url: string (nullable = true)
     |    |    |-- profile_background_image_url_https: string (nullable = true)
     |    |    |-- profile_background_tile: boolean (nullable = true)
     |    |    |-- profile_banner_url: string (nullable = true)
     |    |    |-- profile_image_url: string (nullable = true)
     |    |    |-- profile_image_url_https: string (nullable = true)
     |    |    |-- profile_link_color: string (nullable = true)
     |    |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |    |-- profile_text_color: string (nullable = true)
     |    |    |-- profile_use_background_image: boolean (nullable = true)
     |    |    |-- protected: boolean (nullable = true)
     |    |    |-- screen_name: string (nullable = true)
     |    |    |-- statuses_count: long (nullable = true)
     |    |    |-- time_zone: string (nullable = true)
     |    |    |-- translator_type: string (nullable = true)
     |    |    |-- url: string (nullable = true)
     |    |    |-- utc_offset: long (nullable = true)
     |    |    |-- verified: boolean (nullable = true)
     |-- source: string (nullable = true)
     |-- text: string (nullable = true)
     |-- timestamp_ms: string (nullable = true)
     |-- truncated: boolean (nullable = true)
     |-- user: struct (nullable = true)
     |    |-- contributors_enabled: boolean (nullable = true)
     |    |-- created_at: string (nullable = true)
     |    |-- default_profile: boolean (nullable = true)
     |    |-- default_profile_image: boolean (nullable = true)
     |    |-- description: string (nullable = true)
     |    |-- favourites_count: long (nullable = true)
     |    |-- follow_request_sent: string (nullable = true)
     |    |-- followers_count: long (nullable = true)
     |    |-- following: string (nullable = true)
     |    |-- friends_count: long (nullable = true)
     |    |-- geo_enabled: boolean (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- id_str: string (nullable = true)
     |    |-- is_translator: boolean (nullable = true)
     |    |-- lang: string (nullable = true)
     |    |-- listed_count: long (nullable = true)
     |    |-- location: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- notifications: string (nullable = true)
     |    |-- profile_background_color: string (nullable = true)
     |    |-- profile_background_image_url: string (nullable = true)
     |    |-- profile_background_image_url_https: string (nullable = true)
     |    |-- profile_background_tile: boolean (nullable = true)
     |    |-- profile_banner_url: string (nullable = true)
     |    |-- profile_image_url: string (nullable = true)
     |    |-- profile_image_url_https: string (nullable = true)
     |    |-- profile_link_color: string (nullable = true)
     |    |-- profile_sidebar_border_color: string (nullable = true)
     |    |-- profile_sidebar_fill_color: string (nullable = true)
     |    |-- profile_text_color: string (nullable = true)
     |    |-- profile_use_background_image: boolean (nullable = true)
     |    |-- protected: boolean (nullable = true)
     |    |-- screen_name: string (nullable = true)
     |    |-- statuses_count: long (nullable = true)
     |    |-- time_zone: string (nullable = true)
     |    |-- translator_type: string (nullable = true)
     |    |-- url: string (nullable = true)
     |    |-- utc_offset: long (nullable = true)
     |    |-- verified: boolean (nullable = true)
    


Count the records in the file. Save the result to `dfCount`.


```python
# TODO
dfCount = df.count()
dfCount
```




    1744




```python
# TEST - Run this cell to test your solution
dfTest("ET1-P-08-03-01", 1744, dfCount)

print("Tests passed!")
```

    Tests passed!


## Exercise 2: Defining and Applying a Schema

Applying schemas is especially helpful for data with many fields to sort through. With a complex dataset like this, define a schema **that captures only the relevant fields**.

Capture the hashtags and dates from the data to get a sense for Twitter trends. Use the same file as above.

### Step 1: Understanding the Data Model

In order to apply structure to semi-structured data, you first must understand the data model.  

There are two forms of data models to employ: a relational or non-relational model.<br><br>
* **Relational models** are within the domain of traditional databases. [Normalization](https://en.wikipedia.org/wiki/Database_normalization) is the primary goal of the data model. <br>
* **Non-relational data models** prefer scalability, performance, or flexibility over normalized data.

Use the following relational model to define a number of tables to join together on different columns, in order to reconstitute the original data. Regardless of the data model, the ETL principles are roughly the same.

Compare the following [Entity-Relationship Diagram](https://en.wikipedia.org/wiki/Entity%E2%80%93relationship_model) to the schema you printed out in the previous step to get a sense for how to populate the tables.

<img src="../../resources/ER-diagram.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

### Step 2: Create a Schema for the `Tweet` Table

Create a schema for the JSON data to extract just the information that is needed for the `Tweet` table, parsing each of the following fields in the data model:

| Field | Type|
|-------|-----|
| tweet_id | integer |
| user_id | integer |
| language | string |
| text | string |
| created_at | string* |

*Note: Start with `created_at` as a string. Turn this into a timestamp later.

Save the schema to `tweetSchema`, use it to create a DataFrame named `tweetDF`, and use the same file used in the exercise above: `"s3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4"`.

**Hint:** You might need to reexamine the data schema. <br>
**Hint:** [Import types from `pyspark.sql.types`](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=pyspark%20sql%20types#module-pyspark.sql.types).


```python
# TODO
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

tweetSchema = StructType([
    StructField('id', LongType(), True),
    StructField('user', StructType([
        StructField('id', LongType(), True)
    ])),
    StructField('lang', StringType(), True),
    StructField('text', StringType(), True),
    StructField('created_at', StringType(), True)
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

tweetdf = spark.read.schema(tweetSchema).json(path)
# display(tweetdf)

tweetDF = tweetdf.select(
   F.col("id").alias("tweet_id")
 , F.explode(F.array("user")).alias("user")
 , F.col("user.id").alias("user_id")
 , F.col("lang").alias("language")
 , F.col("text").alias("text")
 , F.col("created_at").alias("created_at")
).drop('user')
display(tweetDF)
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
      <th>tweet_id</th>
      <th>user_id</th>
      <th>language</th>
      <th>text</th>
      <th>created_at</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9.504390e+17</td>
      <td>3.716076e+08</td>
      <td>en</td>
      <td>RT @TheTinaVasquez: Quick facts for the know-n...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9.504390e+17</td>
      <td>7.324171e+08</td>
      <td>ja</td>
      <td>【太ももを引きしめるエクササイズ】足あげ～１．うつぶせに寝る。２．片方の足先を床から10セン...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>4</th>
      <td>9.504390e+17</td>
      <td>2.359272e+08</td>
      <td>tr</td>
      <td>Ben bir beni bulup icine girip saklanirsam kim...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9.504390e+17</td>
      <td>1.564881e+09</td>
      <td>ar</td>
      <td>تواصل قالوا عن قطر المتحدث باسم الجيش الصهيوني...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>6</th>
      <td>9.504390e+17</td>
      <td>3.490704e+08</td>
      <td>en</td>
      <td>*Before you argue about your dirty house someo...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>7</th>
      <td>9.504390e+17</td>
      <td>3.404825e+08</td>
      <td>en</td>
      <td>RT @TippyLexx: Bruh you ever accidentally open...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9.504390e+17</td>
      <td>4.354073e+09</td>
      <td>pt</td>
      <td>RT @MorraoTudo2: A liberdade é só questão de t...</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9.504390e+17</td>
      <td>7.388972e+17</td>
      <td>en</td>
      <td>I just want this all to be over</td>
      <td>Mon Jan 08 18:47:59 +0000 2018</td>
    </tr>
  </tbody>
</table>
</div>




```python
tweetDF.filter(F.col("id").isNotNull()).count()
```




    1491




```python
# TEST - Run this cell to test your solution
from pyspark.sql.functions import col

schema = tweetSchema.fieldNames()
schema.sort()
tweetCount = tweetDF.filter(col("id").isNotNull()).count()

dfTest("ET1-P-08-04-01", 'created_at', schema[0])
dfTest("ET1-P-08-04-02", 'id', schema[1])
dfTest("ET1-P-08-04-03", 1491, tweetCount)

assert schema[0] == 'created_at' and schema[1] == 'id'
assert tweetCount == 1491

print("Tests passed!")
```

    Tests passed!


### Step 3: Create a Schema for the Remaining Tables

Finish off the full schema, save it to `fullTweetSchema`, and use it to create the DataFrame `fullTweetDF`. Your schema should parse all the entities from the ER diagram above.  Remember, smart small, run your code, and then iterate.


```python
# TODO
fullTweetSchema = StructType([
    StructField('id', LongType(), True),                              # tweet id
    StructField('user', StructType([                                  # user [struct]
        StructField('id', LongType(), True),                          # user id
        StructField('screen_name', StringType(), True),               # user screen_name
        StructField('location', StringType(), True),                  # user location
        StructField('friends_count', IntegerType(), True),            # user friends_count
        StructField('followers_count', IntegerType(), True),          # user followers_count
        StructField('description', StringType(), True)                # user description
    ]), True),
    StructField('lang', StringType(), True),                          # tweet lang 
    StructField('text', StringType(), True),                          # tweet text
    StructField('created_at', TimestampType(), True),                 # tweet created_at
    StructField('entities', StructType([                              # tweet entities [struct]
        StructField('hashtags', ArrayType(                            # entities hashtags [array]
            StructType([
                StructField('text', StringType(), True)               # hashtag text
            ])
        ), True),
        StructField('media', ArrayType(                               # tweet media [array]
            StructType([
                StructField('display_url', StringType(), True),       # media display_url
                StructField('expanded_url', StringType(), True),      # media expanded_url
                StructField('url', StringType(), True)                # media url
            ])
        ), True),
    ]), True)
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

fullTweetDF = spark.read.schema(fullTweetSchema).json(path)

fullTweetDF.printSchema()
```

    root
     |-- id: long (nullable = true)
     |-- user: struct (nullable = true)
     |    |-- id: long (nullable = true)
     |    |-- screen_name: string (nullable = true)
     |    |-- location: string (nullable = true)
     |    |-- friends_count: integer (nullable = true)
     |    |-- followers_count: integer (nullable = true)
     |    |-- description: string (nullable = true)
     |-- lang: string (nullable = true)
     |-- text: string (nullable = true)
     |-- created_at: timestamp (nullable = true)
     |-- entities: struct (nullable = true)
     |    |-- hashtags: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- text: string (nullable = true)
     |    |-- media: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
    



```python
# TEST - Run this cell to test your solution
from pyspark.sql.functions import col

schema = fullTweetSchema.fieldNames()
schema.sort()
tweetCount = fullTweetDF.filter(col("id").isNotNull()).count()

assert tweetCount == 1491

dfTest("ET1-P-08-05-01", "created_at", schema[0])
dfTest("ET1-P-08-05-02", "entities", schema[1])
dfTest("ET1-P-08-05-03", 1491, tweetCount)

print("Tests passed!")
```

    Tests passed!


## Exercise 3: Creating the Tables

Apply the schema you defined to create tables that match the relational data model.

### Step 1: Filtering Nulls

The Twitter data contains both deletions and tweets.  This is why some records appear as null values. Create a DataFrame called `fullTweetFilteredDF` that filters out the null values.


```python
# TODO
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

fullTweetFilteredDF = spark.read.json(path).filter(F.col('id').isNotNull())
fullTweetFilteredDF.count()
```




    1491




```python
# TEST - Run this cell to test your solution
dfTest("ET1-P-08-06-01", 1491, fullTweetFilteredDF.count())

print("Tests passed!")
```

    Tests passed!


### Step 2: Creating the `Tweet` Table

Twitter uses a non-standard timestamp format that Spark doesn't recognize. Currently the `created_at` column is formatted as a string. Create the `Tweet` table and save it as `tweetDF`. Parse the timestamp column using `unix_timestamp`, and cast the result as `TimestampType`. The timestamp format is `EEE MMM dd HH:mm:ss ZZZZZ yyyy`.

**Hint:** Use `alias` to alias the name of your columns to the final name you want for them.  
**Hint:** `id` corresponds to `tweet_id` and `user.id` corresponds to `user_id`.


```python
# TODO
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType

tweetSchema = StructType([
    StructField('id', LongType(), True),
    StructField('user', StructType([
        StructField('id', LongType(), True)
    ])),
    StructField('lang', StringType(), True),
    StructField('text', StringType(), True),
    StructField('created_at', StringType(), True)
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

tweetdf = spark.read.schema(tweetSchema).json(path)
# Mon Jan 08 18:47:59 +0000 2018

tweetDF = tweetdf.select(
   F.col("id").alias("tweet_id")
 , F.explode(F.array("user")).alias("user")
 , F.col("user.id").alias("user_id")
 , F.col("lang").alias("language")
 , F.col("text").alias("text")
 , F.unix_timestamp(F.col('created_at'), 'EEE MMM dd HH:mm:ss ZZZZZ yyyy').cast(TimestampType()).alias("createdAt")
).drop('user')
display(tweetDF)

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
      <th>tweet_id</th>
      <th>user_id</th>
      <th>language</th>
      <th>text</th>
      <th>createdAt</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NaN</td>
      <td>NaN</td>
      <td>None</td>
      <td>None</td>
      <td>NaT</td>
    </tr>
    <tr>
      <th>2</th>
      <td>9.504390e+17</td>
      <td>3.716076e+08</td>
      <td>en</td>
      <td>RT @TheTinaVasquez: Quick facts for the know-n...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9.504390e+17</td>
      <td>7.324171e+08</td>
      <td>ja</td>
      <td>【太ももを引きしめるエクササイズ】足あげ～１．うつぶせに寝る。２．片方の足先を床から10セン...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>4</th>
      <td>9.504390e+17</td>
      <td>2.359272e+08</td>
      <td>tr</td>
      <td>Ben bir beni bulup icine girip saklanirsam kim...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>5</th>
      <td>9.504390e+17</td>
      <td>1.564881e+09</td>
      <td>ar</td>
      <td>تواصل قالوا عن قطر المتحدث باسم الجيش الصهيوني...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>6</th>
      <td>9.504390e+17</td>
      <td>3.490704e+08</td>
      <td>en</td>
      <td>*Before you argue about your dirty house someo...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>7</th>
      <td>9.504390e+17</td>
      <td>3.404825e+08</td>
      <td>en</td>
      <td>RT @TippyLexx: Bruh you ever accidentally open...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9.504390e+17</td>
      <td>4.354073e+09</td>
      <td>pt</td>
      <td>RT @MorraoTudo2: A liberdade é só questão de t...</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9.504390e+17</td>
      <td>7.388972e+17</td>
      <td>en</td>
      <td>I just want this all to be over</td>
      <td>2018-01-08 18:47:59</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
from pyspark.sql.types import TimestampType
t = tweetDF.select("createdAt").schema[0]

dfTest("ET1-P-08-07-01", TimestampType(), t.dataType)

print("Tests passed!")
```

    Tests passed!


### Step 3: Creating the Account Table

Save the account table as `accountDF`.


```python
accountSchema = StructType([
    StructField('user', StructType([
        StructField('id', LongType(), True),
        StructField('screen_name', StringType(), True),
        StructField('location', StringType(), True),
        StructField('friends_count', IntegerType(), True),
        StructField('followers_count', IntegerType(), True),
        StructField('description', StringType(), True)
    ]))
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

accountdf = spark.read.schema(accountSchema).json(path)

accountDF = accountdf.select(
   F.explode(F.array("user")).alias("user")
 , F.col("user.id").alias("user_id")
 , F.col("user.screen_name").alias("screenName")
 , F.col("user.location").alias("location")
 , F.col("user.friends_count").alias("friendsCount")
 , F.col("user.followers_count").alias("followersCount")
 , F.col("user.description").alias("description")
).drop('user').filter(F.col('user_id').isNotNull())
display(accountDF)
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
      <th>user_id</th>
      <th>screenName</th>
      <th>location</th>
      <th>friendsCount</th>
      <th>followersCount</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>371607576</td>
      <td>smileifyou_love</td>
      <td>None</td>
      <td>473</td>
      <td>160</td>
      <td>•Psalm 34:18• Living life one day at a time ✌️</td>
    </tr>
    <tr>
      <th>1</th>
      <td>732417055</td>
      <td>bw198e18</td>
      <td>None</td>
      <td>1641</td>
      <td>1285</td>
      <td>【期間限定】今なら無料！！ ただ今話題沸騰中の「ダイエットできるアプリ」こと「ヤセサポ」！！...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>235927210</td>
      <td>marlascigarette</td>
      <td>None</td>
      <td>214</td>
      <td>223</td>
      <td>△</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1564880654</td>
      <td>rebaab_1326</td>
      <td>None</td>
      <td>45</td>
      <td>0</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>349070364</td>
      <td>puskine</td>
      <td>Kampala, Uganda</td>
      <td>5008</td>
      <td>4916</td>
      <td>God first . Football fun . Talk so much . Read...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>340482488</td>
      <td>xNina_Beana</td>
      <td>the land</td>
      <td>1130</td>
      <td>1646</td>
      <td>Prince Carter ❤️ &amp;&amp; Messiah Carter Miles ❤️</td>
    </tr>
    <tr>
      <th>6</th>
      <td>4354072997</td>
      <td>gbfranca22</td>
      <td>cpx da congo🔞</td>
      <td>252</td>
      <td>632</td>
      <td>mãe nunca te escutei, mas sempre te amarei❤</td>
    </tr>
    <tr>
      <th>7</th>
      <td>738897225061912576</td>
      <td>squeeqi</td>
      <td>None</td>
      <td>213</td>
      <td>160</td>
      <td>We are two guys who have great knowledge in sc...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>273646363</td>
      <td>iiib53</td>
      <td>None</td>
      <td>631</td>
      <td>427</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>1541143441</td>
      <td>nappo_what</td>
      <td>🇫🇮🇺🇦</td>
      <td>297</td>
      <td>925</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TODO
```


```python
# TEST - Run this cell to test your solution
cols = accountDF.columns

dfTest("ET1-P-08-08-01", True, "friendsCount" in cols)
dfTest("ET1-P-08-08-02", True, "screenName" in cols)
dfTest("ET1-P-08-08-03", 1491, accountDF.count())


print("Tests passed!")
```

    Tests passed!


### Step 4: Creating Hashtag and URL Tables Using `explode`

Each tweet in the data set contains zero, one, or many URLs and hashtags. Parse these using the `explode` function so that each URL or hashtag has its own row.

In this example, `explode` gives one row from the original column `hashtags` for each value in an array. All other columns are left untouched.

```
+---------------+--------------------+----------------+
|     screenName|            hashtags|explodedHashtags|
+---------------+--------------------+----------------+
|        zooeeen|[[Tea], [GoldenGl...|           [Tea]|
|        zooeeen|[[Tea], [GoldenGl...|  [GoldenGlobes]|
|mannydidthisone|[[beats], [90s], ...|         [beats]|
|mannydidthisone|[[beats], [90s], ...|           [90s]|
|mannydidthisone|[[beats], [90s], ...|     [90shiphop]|
|mannydidthisone|[[beats], [90s], ...|           [pac]|
|mannydidthisone|[[beats], [90s], ...|        [legend]|
|mannydidthisone|[[beats], [90s], ...|          [thug]|
|mannydidthisone|[[beats], [90s], ...|         [music]|
|mannydidthisone|[[beats], [90s], ...|     [westcoast]|
|mannydidthisone|[[beats], [90s], ...|        [eminem]|
|mannydidthisone|[[beats], [90s], ...|         [drdre]|
|mannydidthisone|[[beats], [90s], ...|          [trap]|
|  Satish0919995|[[BB11], [BiggBos...|          [BB11]|
|  Satish0919995|[[BB11], [BiggBos...|    [BiggBoss11]|
|  Satish0919995|[[BB11], [BiggBos...| [WeekendKaVaar]|
+---------------+--------------------+----------------+
```

The concept of `explode` is similar to `pivot`.

Create the rest of the tables and save them to the following DataFrames:<br><br>

* `hashtagDF`
* `urlDF`

<a href="http://spark.apache.org/docs/latest/api/python/pyspark.sql.html?highlight=explode#pyspark.sql.functions.explode" target="_blank">Find the documentation for `explode` here</a>

#### URL Table


```python
from pyspark.sql.types import ArrayType

urlSchema = StructType([
    StructField('entities', StructType([
        StructField('urls', ArrayType(
            StructType([
                StructField('display_url', StringType(), True),
                StructField('expanded_url', StringType(), True),
                StructField('url', StringType(), True)
            ])
        ), True),
    ]), True),
    StructField('id', LongType(), True)
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

urldf = spark.read.schema(urlSchema).json(path)
urlDF = urldf.select(
      F.col('id').alias('tweet_id')
    , F.explode(('entities.urls')).alias('element')
    , F.col('element.display_url').alias('displayURL')
    , F.col('element.expanded_url').alias('expandedURL')
    , F.col('element.url').alias('URL')
).drop('element')

urldf.printSchema()
display(urlDF)
```

    root
     |-- entities: struct (nullable = true)
     |    |-- urls: array (nullable = true)
     |    |    |-- element: struct (containsNull = true)
     |    |    |    |-- display_url: string (nullable = true)
     |    |    |    |-- expanded_url: string (nullable = true)
     |    |    |    |-- url: string (nullable = true)
     |-- id: long (nullable = true)
    





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
      <th>tweet_id</th>
      <th>displayURL</th>
      <th>expandedURL</th>
      <th>URL</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>950438954280472576</td>
      <td>youtube.com/watch?v=b4iz9n…</td>
      <td>https://www.youtube.com/watch?v=b4iz9nZPzAA</td>
      <td>https://t.co/j0RgDwS36n</td>
    </tr>
    <tr>
      <th>1</th>
      <td>950438954284797958</td>
      <td>twitter.com/i/web/status/9…</td>
      <td>https://twitter.com/i/web/status/9504389542847...</td>
      <td>https://t.co/B5Zgkoy4TL</td>
    </tr>
    <tr>
      <th>2</th>
      <td>950438954310033410</td>
      <td>bit.ly/OYlKII</td>
      <td>http://bit.ly/OYlKII</td>
      <td>http://t.co/Kv3EWEhO</td>
    </tr>
    <tr>
      <th>3</th>
      <td>950438954305835008</td>
      <td>goo.gl/fb/atjACB</td>
      <td>https://goo.gl/fb/atjACB</td>
      <td>https://t.co/l3x0sVSvFa</td>
    </tr>
    <tr>
      <th>4</th>
      <td>950438954305761280</td>
      <td>instagram.com/p/BdsvNFABXNL/</td>
      <td>https://www.instagram.com/p/BdsvNFABXNL/</td>
      <td>https://t.co/syjjduK5w0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>950438954301644800</td>
      <td>twitter.com/i/web/status/9…</td>
      <td>https://twitter.com/i/web/status/9504389543016...</td>
      <td>https://t.co/MfiddAKVoK</td>
    </tr>
    <tr>
      <th>6</th>
      <td>950438954280550407</td>
      <td>alathkar.org</td>
      <td>http://alathkar.org</td>
      <td>https://t.co/5C60KO3ODg</td>
    </tr>
    <tr>
      <th>7</th>
      <td>950438954288996354</td>
      <td>open.spotify.com/album/60eldei5…</td>
      <td>https://open.spotify.com/album/60eldei5NAr7QjP...</td>
      <td>https://t.co/diiJEdOPni</td>
    </tr>
    <tr>
      <th>8</th>
      <td>950438954301616131</td>
      <td>twitter.com/i/web/status/9…</td>
      <td>https://twitter.com/i/web/status/9504389543016...</td>
      <td>https://t.co/Ram2cKemaW</td>
    </tr>
    <tr>
      <th>9</th>
      <td>950438954280587264</td>
      <td>twitter.com/frann_frann_/s…</td>
      <td>https://twitter.com/frann_frann_/status/949080...</td>
      <td>https://t.co/8IVHNy4MV7</td>
    </tr>
  </tbody>
</table>
</div>




```python
urlDF.count()
```




    368



#### HashtagTable


```python
haghtagSchema = StructType([
    StructField('entities', StructType([
        StructField('hashtags', ArrayType(
            StructType([
                StructField('text', StringType(), True)
            ])
        ), True),
    ]), True),
    StructField('id', LongType(), True)
])
```


```python
path = 's3://data.intellinum.co/bootcamp/common/twitter/firehose/2018/01/08/18/twitterstream-1-2018-01-08-18-48-00-bcf3d615-9c04-44ec-aac9-25f966490aa4'

hashtagdf = spark.read.schema(haghtagSchema).json(path)

hashtagDF = hashtagdf.select(
      F.col('id').alias('tweet_id')
    , F.explode('entities.hashtags').alias('hashtags')
    , F.col('hashtags.text').alias('hashtag')
).drop('hashtags')

display(hashtagDF)
```


```python
# TEST - Run this cell to test your solution
hashtagCols = hashtagDF.columns
urlCols = urlDF.columns
hashtagDFCounts = hashtagDF.count()
urlDFCounts = urlDF.count()

dfTest("ET1-P-08-09-01", True, "hashtag" in hashtagCols)
dfTest("ET1-P-08-09-02", True, "displayURL" in urlCols)
dfTest("ET1-P-08-09-03", 394, hashtagDFCounts)
dfTest("ET1-P-08-09-04", 368, urlDFCounts)

print("Tests passed!")
```

    Tests passed!


## Exercise 4: Loading the Results

Use S3 as your target warehouse for your transformed data. Save the DataFrames in Parquet format to the following endpoints:  

| DataFrame    | Endpoint                                 |
|:-------------|:-----------------------------------------|
| `accountDF`  | `"s3a://temp.intellinum.co/" + username + "/account.parquet"`|
| `tweetDF`    | `"s3a://temp.intellinum.co/" + username + "/tweet.parquet"`  |
| `hashtagDF`  | `"s3a://temp.intellinum.co/" + username + "/hashtag.parquet"`|
| `urlDF`      | `"s3a://temp.intellinum.co/" + username + "/url.parquet"`    |

If you run out of storage in `/tmp`, use `.limit(10)` to limit the size of your DataFrames to 10 records.


```python
!aws s3 ls temp.intellinum.co/
```

    
    An error occurred (NoSuchBucket) when calling the ListObjectsV2 operation: The specified bucket does not exist



```python
# TODO
YOUR_FIRST_NAME = 'rajeev'

accountDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/account.parquet")
tweetDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/tweet.parquet")
hashtagDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/hashtag.parquet")
urlDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/url.parquet")

```


    ---------------------------------------------------------------------------

    Py4JJavaError                             Traceback (most recent call last)

    <ipython-input-39-bb7513e7d46c> in <module>
          2 YOUR_FIRST_NAME = 'rajeev'
          3 
    ----> 4 accountDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/account.parquet")
          5 tweetDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/tweet.parquet")
          6 hashtagDF.repartition(1).write.mode("overwrite").parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/hashtag.parquet")


    ~/anaconda/envs/pyspark24_py36/lib/python3.6/site-packages/pyspark/sql/readwriter.py in parquet(self, path, mode, partitionBy, compression)
        839             self.partitionBy(partitionBy)
        840         self._set_opts(compression=compression)
    --> 841         self._jwrite.parquet(path)
        842 
        843     @since(1.6)


    ~/anaconda/envs/pyspark24_py36/lib/python3.6/site-packages/py4j/java_gateway.py in __call__(self, *args)
       1255         answer = self.gateway_client.send_command(command)
       1256         return_value = get_return_value(
    -> 1257             answer, self.gateway_client, self.target_id, self.name)
       1258 
       1259         for temp_arg in temp_args:


    ~/anaconda/envs/pyspark24_py36/lib/python3.6/site-packages/pyspark/sql/utils.py in deco(*a, **kw)
         61     def deco(*a, **kw):
         62         try:
    ---> 63             return f(*a, **kw)
         64         except py4j.protocol.Py4JJavaError as e:
         65             s = e.java_exception.toString()


    ~/anaconda/envs/pyspark24_py36/lib/python3.6/site-packages/py4j/protocol.py in get_return_value(answer, gateway_client, target_id, name)
        326                 raise Py4JJavaError(
        327                     "An error occurred while calling {0}{1}{2}.\n".
    --> 328                     format(target_id, ".", name), value)
        329             else:
        330                 raise Py4JError(


    Py4JJavaError: An error occurred while calling o947.parquet.
    : java.io.FileNotFoundException: Bucket temp.intellinum.co does not exist
    	at org.apache.hadoop.fs.s3a.S3AFileSystem.verifyBucketExists(S3AFileSystem.java:277)
    	at org.apache.hadoop.fs.s3a.S3AFileSystem.initialize(S3AFileSystem.java:236)
    	at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:2859)
    	at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:99)
    	at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:2896)
    	at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:2878)
    	at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:392)
    	at org.apache.hadoop.fs.Path.getFileSystem(Path.java:356)
    	at org.apache.spark.sql.execution.datasources.DataSource.planForWritingFileFormat(DataSource.scala:424)
    	at org.apache.spark.sql.execution.datasources.DataSource.planForWriting(DataSource.scala:524)
    	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:290)
    	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:271)
    	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:229)
    	at org.apache.spark.sql.DataFrameWriter.parquet(DataFrameWriter.scala:566)
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




```python
!aws s3 ls temp.intellinum.co/rajeev/
```

    
    An error occurred (NoSuchBucket) when calling the ListObjectsV2 operation: The specified bucket does not exist



```python
# TEST - Run this cell to test your solution
from pyspark.sql.dataframe import DataFrame

accountDF = spark.read.parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/account.parquet")
tweetDF = spark.read.parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/tweet.parquet")
hashtagDF = spark.read.parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/hashtag.parquet")
urlDF = spark.read.parquet("s3a://temp.intellinum.co/" + YOUR_FIRST_NAME + "/url.parquet")

dfTest("ET1-P-08-10-01", DataFrame, type(accountDF))
dfTest("ET1-P-08-10-02", DataFrame, type(tweetDF))
dfTest("ET1-P-08-10-03", DataFrame, type(hashtagDF))
dfTest("ET1-P-08-10-04", DataFrame, type(urlDF))

print("Tests passed!")
```

## Congratulations, you have completed ETL Part 1!

Before you go, please clear contents in the temp folder


```python
!aws s3 rm --recursive {f"s3://temp.intellinum.co/%s" % YOUR_FIRST_NAME}
```

    delete: s3://temp.intellinum.co/rajeev/account.parquet/_SUCCESS
    delete: s3://temp.intellinum.co/rajeev/tweet.parquet/part-00000-5475e11a-7597-4c8d-b533-cbb42a55a3ac-c000.snappy.parquet
    delete: s3://temp.intellinum.co/rajeev/hashtag.parquet/part-00000-14195eca-26ca-4be3-b960-135ce17709dd-c000.snappy.parquet
    delete: s3://temp.intellinum.co/rajeev/account.parquet/part-00000-f3064da7-0dec-404e-9868-8c7d5f077595-c000.snappy.parquet
    delete: s3://temp.intellinum.co/rajeev/url.parquet/_SUCCESS
    delete: s3://temp.intellinum.co/rajeev/hashtag.parquet/_SUCCESS
    delete: s3://temp.intellinum.co/rajeev/url.parquet/part-00000-dd30bd9e-ef1c-4319-870e-ca397ef7fb12-c000.snappy.parquet
    delete: s3://temp.intellinum.co/rajeev/tweet.parquet/_SUCCESS


&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
