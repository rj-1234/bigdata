
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# ETL Part 1: Data Extraction

In this course data engineers access data where it lives and then apply data extraction best practices, including schemas, corrupt record handling, and parallelized code. By the end of this course, you will extract data from multiple sources, use schema inference and apply user-defined schemas, and navigate Spark documents to source solutions.

** The course is composed of the following lessons:**  
1. Course Overview and Setup
2. ETL Process Overview
3. Connecting to S3
4. Connecting to JDBC
5. Applying Schemas to JSON Data
6. Corrupt Record Handling
7. Loading Data and Productionalizing
8. Milestone Project 


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


### ETL with Spark

The **extract, transform, load (ETL)** process takes data from one or more sources, transforms it, normally by adding structure, and then loads it into a target database. 

A common ETL job takes log files from a web server, parses out pertinent fields so it can be readily queried, and then loads it into a database.

ETL may seem simple: applying structure to data so it’s in a desired form. However, the complexity of ETL is in the details. Data Engineers building ETL pipelines must understand and apply the following concepts:<br><br>

* Optimizing data formats and connections
* Determining the ideal schema
* Handling corrupt records
* Automating workloads

This course addresses these concepts.

<img src="../../resources/ETL-overview.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

Stay tuned for upcoming courses which will cover:<br><br>

* Complex and performant data transformations
* Schema changes over time  
* Recovery from job failures
* Avoiding duplicate records

## ETL Process Overview

Apache Spark&trade; allow you to create an end-to-end _extract, transform, load (ETL)_ pipeline.
## In this lesson you:
* Create a basic end-to-end ETL pipeline
* Demonstrate the Spark approach to ETL pipelines

### The Spark Approach

Spark offers a compute engine and connectors to virtually any data source. By leveraging easily scaled infrastructure and accessing data where it lives, Spark addresses the core needs of a big data application.

These principles comprise the Spark approach to ETL, providing a unified and scalable approach to big data pipelines: <br><br>

1. Spark offer a **unified platform** 
 - Spark combines ETL, stream processing, machine learning, and collaborative notebooks.
 - Data scientists, analysts, and engineers can write Spark code in Python, Scala, SQL, and R.
2. Spark's unified platform is **scalable to petabytes of data and clusters of thousands of nodes**.  
 - The same code written on smaller data sets scales to large workloads, often with only small changes.
2. Spark decouples data storage from the compute and query engine.  
 - Spark's query engine **connects to any number of data sources** such as S3, Azure Blob Storage, Redshift, and Kafka.  
 - This **minimizes costs**; a dedicated cluster does not need to be maintained and the compute cluster is **easily updated to the latest version** of Spark.
 
<img src="../../resources/Workload_Tools_2-01.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>
 

### A Basic ETL Job

In this lesson you use web log files from the <a href="https://www.sec.gov/dera/data/edgar-log-file-data-set.html" target="_blank">US Securities and Exchange Commission website</a> to do a basic ETL for a day of server activity. You will extract the fields of interest and load them into persistent storage.


```python
path = "s3a://data.intellinum.co/bootcamp/common/EDGAR-Log-20170329/EDGAR-Log-20170329.csv"

logDF = (spark
  .read
  .option("header", True)
  .csv(path)
  .sample(withReplacement=False, fraction=0.3, seed=3) # using a sample to reduce data size
)

display(logDF)
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
    </tr>
  </tbody>
</table>
</div>



Next, review the server-side errors, which have error codes in the 500s.  


```python
from pyspark.sql.functions import col

serverErrorDF = (logDF
  .filter((col("code") >= 500) & (col("code") < 600))
  .select("date", "time", "extention", "code")
)

display(serverErrorDF)
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
      <th>date</th>
      <th>time</th>
      <th>extention</th>
      <th>code</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2017-03-29</td>
      <td>00:00:12</td>
      <td>.txt</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2017-03-29</td>
      <td>00:00:16</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2017-03-29</td>
      <td>00:00:24</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2017-03-29</td>
      <td>00:00:44</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2017-03-29</td>
      <td>00:01:01</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>2017-03-29</td>
      <td>00:01:01</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>2017-03-29</td>
      <td>00:01:02</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>2017-03-29</td>
      <td>00:01:03</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>2017-03-29</td>
      <td>00:01:03</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>2017-03-29</td>
      <td>00:01:04</td>
      <td>-index.htm</td>
      <td>503.0</td>
    </tr>
  </tbody>
</table>
</div>



### Data Validation

One aspect of ETL jobs is to validate that the data is what you expect.  This includes:<br><br>
* Approximately the expected number of records
* The expected fields are present
* No unexpected missing values

Take a look at the server-side errors by hour to confirm the data meets your expectations. Visualize it by using matplotlib or plotly. <br><br>


```python
from pyspark.sql.functions import from_utc_timestamp, hour, col

countsDF = (serverErrorDF
  .select(hour(from_utc_timestamp(col("time"), "GMT")).alias("hour"))
  .groupBy("hour")
  .count()
  .orderBy("hour")
)

display(countsDF, 20)
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
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>2094</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>1598</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>1136</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>1027</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>1135</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>1182</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
      <td>1109</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>1004</td>
    </tr>
    <tr>
      <th>8</th>
      <td>8</td>
      <td>1127</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
      <td>1053</td>
    </tr>
    <tr>
      <th>10</th>
      <td>10</td>
      <td>1063</td>
    </tr>
    <tr>
      <th>11</th>
      <td>11</td>
      <td>1099</td>
    </tr>
    <tr>
      <th>12</th>
      <td>12</td>
      <td>1008</td>
    </tr>
    <tr>
      <th>13</th>
      <td>13</td>
      <td>1099</td>
    </tr>
    <tr>
      <th>14</th>
      <td>14</td>
      <td>1141</td>
    </tr>
    <tr>
      <th>15</th>
      <td>15</td>
      <td>1248</td>
    </tr>
    <tr>
      <th>16</th>
      <td>16</td>
      <td>1275</td>
    </tr>
    <tr>
      <th>17</th>
      <td>17</td>
      <td>1384</td>
    </tr>
    <tr>
      <th>18</th>
      <td>18</td>
      <td>1172</td>
    </tr>
    <tr>
      <th>19</th>
      <td>19</td>
      <td>1244</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TODO
import plotly.graph_objs as go

errorByHourDF = countsDF.toPandas()

plotData = go.Bar(
                x = errorByHourDF["hour"],
                y = errorByHourDF["count"],
                name = "Error code count by the hour",
            )

layout = go.Layout(
                title="Error code count by the hour",
                xaxis={
                    "title" : "Hour",
                    "tickfont" : {
                        "size" : 14,
                    }
                },
                yaxis={
                    "title" : "Count",
                    "tickfont" : {
                        "size" : 10,
                    }
                },
                legend = {
                    "x" : 1,
                    "y" : 1
                },
                barmode="group",
                bargap=0.15,
                bargroupgap=0.1
            )

fig = go.Figure(data=[plotData], layout=layout)
plotly.offline.iplot(fig)
```


<div>
        
        
            <div id="d5485d74-546b-4e32-b2e0-db3004da6b83" class="plotly-graph-div" style="height:525px; width:100%;"></div>
            <script type="text/javascript">
                require(["plotly"], function(Plotly) {
                    window.PLOTLYENV=window.PLOTLYENV || {};
                    window.PLOTLYENV.BASE_URL='https://plot.ly';
                    
                if (document.getElementById("d5485d74-546b-4e32-b2e0-db3004da6b83")) {
                    Plotly.newPlot(
                        'd5485d74-546b-4e32-b2e0-db3004da6b83',
                        [{"name": "Error code count by the hour", "type": "bar", "uid": "c21c5ce7-45ab-4933-888b-151e9ef6659d", "x": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23], "y": [2094, 1598, 1136, 1027, 1135, 1182, 1109, 1004, 1127, 1053, 1063, 1099, 1008, 1099, 1141, 1248, 1275, 1384, 1172, 1244, 1279, 1130, 1077, 1090]}],
                        {"bargap": 0.15, "bargroupgap": 0.1, "barmode": "group", "legend": {"x": 1, "y": 1}, "title": {"text": "Error code count by the hour"}, "xaxis": {"tickfont": {"size": 14}, "title": {"text": "Hour"}}, "yaxis": {"tickfont": {"size": 10}, "title": {"text": "Count"}}},
                        {"showLink": false, "linkText": "Export to plot.ly", "plotlyServerURL": "https://plot.ly", "responsive": true}
                    ).then(function(){
                            
var gd = document.getElementById('d5485d74-546b-4e32-b2e0-db3004da6b83');
var x = new MutationObserver(function (mutations, observer) {{
        var display = window.getComputedStyle(gd).display;
        if (!display || display === 'none') {{
            console.log([gd, 'removed!']);
            Plotly.purge(gd);
            observer.disconnect();
        }}
}});

// Listen for the removal of the full notebook cells
var notebookContainer = gd.closest('#notebook-container');
if (notebookContainer) {{
    x.observe(notebookContainer, {childList: true});
}}

// Listen for the clearing of the current output cell
var outputEl = gd.closest('.output');
if (outputEl) {{
    x.observe(outputEl, {childList: true});
}}

                        })
                };
                });
            </script>
        </div>


The distribution of errors by hour meets the expectations.  There is an uptick in errors around midnight, possibly due to server maintenance at this time.

### Saving Back to S3

A common and highly effective design pattern in the Spark ecosystem involves loading structured data back to S3 as a parquet file. Learn more about [the scalable and optimized data storage format parquet here](http://parquet.apache.org/).

Save the parsed DataFrame back to S3 as parquet using the `.write` method.


```python
# TODO
username = "rajeev"
(serverErrorDF
  .write
  .mode("overwrite") # overwrites a file if it already exists
  .parquet("s3a://temp.intellinum.co/" + username + "/serverErrorDF.parquet")
)
```

### Our ETL Pipeline

Here's what the ETL pipeline you just built looks like.  In the rest of this course you will work with more complex versions of this general pattern.

| Code | Stage |
|:------|:-------|
| `logDF = (spark`                                                                          | Extract |
| &nbsp;&nbsp;&nbsp;&nbsp;`.read`                                                           | Extract |
| &nbsp;&nbsp;&nbsp;&nbsp;`.option("header", True)`                                         | Extract |
| &nbsp;&nbsp;&nbsp;&nbsp;`.csv(<source>)`                                                  | Extract |
| `)`                                                                                       | Extract |
| `serverErrorDF = (logDF`                                                                  | Transform |
| &nbsp;&nbsp;&nbsp;&nbsp;`.filter((col("code") >= 500) & (col("code") < 600))`             | Transform |
| &nbsp;&nbsp;&nbsp;&nbsp;`.select("date", "time", "extention", "code")`                    | Transform |
| `)`                                                                                       | Transform |
| `(serverErrorDF.write`                                                                 | Load |
| &nbsp;&nbsp;&nbsp;&nbsp;`.parquet(<destination>))`                                      | Load |

This is a distributed job, so it can easily scale to fit the demands of your data set.

## Exercise 1: Perform an ETL Job

Write a basic ETL script that captures the 20 most active website users and load the results to DBFS.


```python
#TODO
from pyspark.sql.functions import desc, count

ipCountDF = (logDF.groupBy("ip").agg(count("ip").alias("count")).orderBy(desc("count")))
```


```python
display(ipCountDF)
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
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>213.152.28.bhe</td>
      <td>520522</td>
    </tr>
    <tr>
      <th>1</th>
      <td>158.132.91.haf</td>
      <td>497817</td>
    </tr>
    <tr>
      <th>2</th>
      <td>117.91.6.caf</td>
      <td>239925</td>
    </tr>
    <tr>
      <th>3</th>
      <td>132.195.122.djf</td>
      <td>197673</td>
    </tr>
    <tr>
      <th>4</th>
      <td>117.91.2.aha</td>
      <td>152204</td>
    </tr>
    <tr>
      <th>5</th>
      <td>173.52.208.ehd</td>
      <td>146972</td>
    </tr>
    <tr>
      <th>6</th>
      <td>108.91.91.hbc</td>
      <td>142731</td>
    </tr>
    <tr>
      <th>7</th>
      <td>117.91.7.hgh</td>
      <td>133579</td>
    </tr>
    <tr>
      <th>8</th>
      <td>97.100.78.cjb</td>
      <td>129753</td>
    </tr>
    <tr>
      <th>9</th>
      <td>217.174.255.dgd</td>
      <td>123177</td>
    </tr>
  </tbody>
</table>
</div>




```python
top20IpDF = ipCountDF.limit(20).toPandas()

plotData = go.Bar(
                x = top20IpDF["ip"],
                y = top20IpDF["count"],
                name = "Most Active Users",
            )

layout = go.Layout(
                title="Most Active Users",
                xaxis={
                    "title" : "Hour",
                    "tickfont" : {
                        "size" : 14,
                    }
                },
                yaxis={
                    "title" : "Count",
                    "tickfont" : {
                        "size" : 10,
                    }
                },
                legend = {
                    "x" : 1,
                    "y" : 1
                },
                barmode="group",
                bargap=0.15,
                bargroupgap=0.1
            )

fig = go.Figure(data=[plotData], layout=layout)
plotly.offline.iplot(fig)
```


<div>
        
        
            <div id="69808c50-661c-4798-856b-85a77d8ebb94" class="plotly-graph-div" style="height:525px; width:100%;"></div>
            <script type="text/javascript">
                require(["plotly"], function(Plotly) {
                    window.PLOTLYENV=window.PLOTLYENV || {};
                    window.PLOTLYENV.BASE_URL='https://plot.ly';
                    
                if (document.getElementById("69808c50-661c-4798-856b-85a77d8ebb94")) {
                    Plotly.newPlot(
                        '69808c50-661c-4798-856b-85a77d8ebb94',
                        [{"name": "Most Active Users", "type": "bar", "uid": "7fde0953-c86e-44ca-a041-c96881a33e7e", "x": ["213.152.28.bhe", "158.132.91.haf", "117.91.6.caf", "132.195.122.djf", "117.91.2.aha", "173.52.208.ehd", "108.91.91.hbc", "117.91.7.hgh", "97.100.78.cjb", "217.174.255.dgd", "64.140.243.cgg", "117.91.6.jfd", "117.91.7.bhf", "54.69.84.iji", "54.212.94.jcd", "101.231.120.dfg", "54.92.220.eij", "132.200.132.fdh", "117.91.6.dgd", "116.231.50.gae"], "y": [520522, 497817, 239925, 197673, 152204, 146972, 142731, 133579, 129753, 123177, 121052, 119873, 111371, 110040, 104801, 89679, 84153, 82639, 72290, 67585]}],
                        {"bargap": 0.15, "bargroupgap": 0.1, "barmode": "group", "legend": {"x": 1, "y": 1}, "title": {"text": "Most Active Users"}, "xaxis": {"tickfont": {"size": 14}, "title": {"text": "Hour"}}, "yaxis": {"tickfont": {"size": 10}, "title": {"text": "Count"}}},
                        {"showLink": false, "linkText": "Export to plot.ly", "plotlyServerURL": "https://plot.ly", "responsive": true}
                    ).then(function(){
                            
var gd = document.getElementById('69808c50-661c-4798-856b-85a77d8ebb94');
var x = new MutationObserver(function (mutations, observer) {{
        var display = window.getComputedStyle(gd).display;
        if (!display || display === 'none') {{
            console.log([gd, 'removed!']);
            Plotly.purge(gd);
            observer.disconnect();
        }}
}});

// Listen for the removal of the full notebook cells
var notebookContainer = gd.closest('#notebook-container');
if (notebookContainer) {{
    x.observe(notebookContainer, {childList: true});
}}

// Listen for the clearing of the current output cell
var outputEl = gd.closest('.output');
if (outputEl) {{
    x.observe(outputEl, {childList: true});
}}

                        })
                };
                });
            </script>
        </div>



```python
# TEST - Run this cell to test your solution
ip1, count1 = ipCountDF.first()
cols = set(ipCountDF.columns)

dfTest("ET1-P-02-01-01", "213.152.28.bhe", ip1)
dfTest("ET1-P-02-01-02", True, count1 > 500000 and count1 < 550000)
dfTest("ET1-P-02-01-03", True, 'count' in cols)
dfTest("ET1-P-02-01-03", True, 'ip' in cols)

print("Tests passed!")
```

    Tests passed!


### Step 2: Save the Results

Use your temporary folder to save the results back to S3 as `"s3a://temp.intellinum.co/" + username + "/ipCount.parquet"`




```python
# TODO
username = "rajeev"
path = "s3a://temp.intellinum.co/" + username + "/ipCount.parquet"
(ipCountDF
    .write
    .mode("overwrite")
    .parquet(path))
```


```python
# TEST - Run this cell to test your solution
from pyspark.sql.functions import desc

username = "rajeev"
writePath = "s3a://temp.intellinum.co/" + username + "/ipCount.parquet"

ipCountDF2 = (spark
  .read
  .parquet(writePath)
  .orderBy(desc("count"))
)
ip1, count1 = ipCountDF2.first()
cols = ipCountDF2.columns

dfTest("ET1-P-02-02-01", "213.152.28.bhe", ip1)
dfTest("ET1-P-02-02-02", True, count1 > 500000 and count1 < 550000)
dfTest("ET1-P-02-02-03", True, "count" in cols)
dfTest("ET1-P-02-02-04", True, "ip" in cols)

print("Tests passed!")
```

    Tests passed!


Check the load worked by using `aws s3 ls <path>`.  Parquet divides your data into a number of files.  If successful, you see a `_SUCCESS` file as well as the data split across a number of parts.


```python
!aws s3 ls {writePath.replace('s3a','s3')}
```

                               PRE ipCount.parquet/


## Review
**Question:** What does ETL stand for and what are the stages of the process?  
**Answer:** ETL stands for `extract-transform-load`
0. *Extract* refers to ingesting data.  Spark easily connects to data in a number of different sources.
0. *Transform* refers to applying structure, parsing fields, cleaning data, and/or computing statistics.
0. *Load* refers to loading data to its final destination, usually a database or data warehouse.

**Question:** How does the Spark approach to ETL deal with devops issues such as updating a software version?  
**Answer:** By decoupling storage and compute, updating your Spark version is as easy as spinning up a new cluster.  Your old code will easily connect to S3, the Azure Blob, or other storage.  This also avoids the challenge of keeping a cluster always running, such as with Hadoop clusters.

**Question:** How does the Spark approach to data applications differ from other solutions?  
**Answer:** Spark offers a unified solution to use cases that would otherwise need individual tools. For instance, Spark combines machine learning, ETL, stream processing, and a number of other solutions all with one technology.

## Additional Topics & Resources

**Q:** Where can I get more information on building ETL pipelines?  
**A:** Check out the Spark Summit talk on <a href="https://databricks.com/session/building-robust-etl-pipelines-with-apache-spark" target="_blank">Building Robust ETL Pipelines with Apache Spark</a>

**Q:** Where can I find out more information on moving from traditional ETL pipelines towards Spark?  
**A:** Check out the Spark Summit talk <a href="https://databricks.com/session/get-rid-of-traditional-etl-move-to-spark" target="_blank">Get Rid of Traditional ETL, Move to Spark!</a>

**Q:** What are the visualization options?  
**A:** 3rd party visualization libraries, including <a href="https://d3js.org/" target="_blank">d3.js</a>, <a href="https://matplotlib.org/" target="_blank">matplotlib</a>, <a href="http://ggplot.yhathq.com/" target="_blank">ggplot</a>, and <a href="https://plot.ly/" target="_blank">plotly<a/>.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
