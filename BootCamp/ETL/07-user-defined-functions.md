
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# User Defined Functions

Apache Spark&trade; allows you to create your own User Defined Functions (UDFs) specific to the needs of your data.

## In this lesson you:
* Write UDFs with a single DataFrame column inputs
* Transform data using UDFs using both the DataFrame and SQL API
* Analyze the performance trade-offs between built-in functions and UDFs


### Custom Transformations with User Defined Functions

Spark's built-in functions provide a wide array of functionality, covering the vast majority of data transformation use cases. Often what differentiates strong Spark programmers is their ability to utilize built-in functions since Spark offers many highly optimized options to manipulate data. This matters for two reasons:<br><br>

- First, *built-in functions are finely tuned* so they run faster than less efficient code provided by the user.  
- Secondly, Spark (or, more specifically, Spark's optimization engine, the Catalyst Optimizer) knows the objective of built-in functions so it can *optimize the execution of your code by changing the order of your tasks.* 

In brief, use built-in functions whenever possible.

There are, however, many specific use cases not covered by built-in functions. **User Defined Functions (UDFs) are useful when you need to define logic specific to your use case and when you need to encapsulate that solution for reuse.** They should only be used when there is no clear way to accomplish a task using built-in functions.

UDFs are generally more performant in Scala than Python since for Python, Spark has to spin up a Python interpreter on every executor to run the function. This causes a substantial performance bottleneck due to communication across the Py4J bridge (how the JVM inter-operates with Python) and the slower nature of Python execution.

<div><img src="../../resources/built-in-vs-udfs.png" style="height: 400px; margin: 20px"/></div>

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
            setAppName("pyspark_etl_07-udf").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_07-udf-rajeev").\
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


### A Basic UDF

UDFs take a function or lambda and make it available for Spark to use.  Start by writing code in your language of choice that will operate on a single row of a single column in your DataFrame.

Write a basic function that splits a string on an `e`.


```python
def manual_split(x):
    return x.split("e")

manual_split("this is my example string")
```




    ['this is my ', 'xampl', ' string']



Register the function as a UDF by designating the following:

* A name for access in Python (`manualSplitPythonUDF`)
* A name for access in SQL (`manualSplitSQLUDF`)
* The function itself (`manual_split`)
* The return type for the function (`StringType`)


```python
from pyspark.sql.types import StringType

manualSplitPythonUDF = spark.udf.register("manualSplitSQLUDF", manual_split, StringType())
```

Create a DataFrame of 100k values with a string to index. Do this by using a hash function, in this case `SHA-1`.


```python
from pyspark.sql.functions import sha1, rand
randomDF = (spark.range(1, 10000 * 10 * 10 * 10)
  .withColumn("random_value", rand(seed=10).cast("string"))
  .withColumn("hash", sha1("random_value"))
  .drop("random_value")
)

display(randomDF)
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
      <th>hash</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>6f65a37773e1a173aefe023667fe23442efaf0e5</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3c5f154011aa08151d2cc5b458a48b33ab656c67</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>28fc878d1f1942bd718cefa60424d9e18e8756db</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>9c54ebb1c11c92d4ce7c2d52a15d21c337e0ced5</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>46500976da0a25029a93419acd36935d06c86e95</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>9d8e41374ef6637e274e4b5487a6428b72b5fb2a</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>29d9fa515d557568abc85d08d1217b6b21c4841d</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>672da7735c57d6707a8307bd18b1fd4dc04821ee</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>e6fb1c43359cff2d70014e776bd47565bdbf40a2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>8e3f2b2dfae9a25aa4a940067e34bf4cebde5926</td>
    </tr>
  </tbody>
</table>
</div>



Apply the UDF by using it just like any other Spark function.


```python
randomAugmentedDF = randomDF.select("*", manualSplitPythonUDF("hash").alias("augmented_col"))

display(randomAugmentedDF)
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
      <th>hash</th>
      <th>augmented_col</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>6f65a37773e1a173aefe023667fe23442efaf0e5</td>
      <td>[6f65a37773, 1a173a, f, 023667f, 23442, faf0, 5]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3c5f154011aa08151d2cc5b458a48b33ab656c67</td>
      <td>[3c5f154011aa08151d2cc5b458a48b33ab656c67]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>28fc878d1f1942bd718cefa60424d9e18e8756db</td>
      <td>[28fc878d1f1942bd718c, fa60424d9, 18, 8756db]</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>9c54ebb1c11c92d4ce7c2d52a15d21c337e0ced5</td>
      <td>[9c54, bb1c11c92d4c, 7c2d52a15d21c337, 0c, d5]</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>46500976da0a25029a93419acd36935d06c86e95</td>
      <td>[46500976da0a25029a93419acd36935d06c86, 95]</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>9d8e41374ef6637e274e4b5487a6428b72b5fb2a</td>
      <td>[9d8, 41374, f6637, 274, 4b5487a6428b72b5fb2a]</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>29d9fa515d557568abc85d08d1217b6b21c4841d</td>
      <td>[29d9fa515d557568abc85d08d1217b6b21c4841d]</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>672da7735c57d6707a8307bd18b1fd4dc04821ee</td>
      <td>[672da7735c57d6707a8307bd18b1fd4dc04821, , ]</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>e6fb1c43359cff2d70014e776bd47565bdbf40a2</td>
      <td>[, 6fb1c43359cff2d70014, 776bd47565bdbf40a2]</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>8e3f2b2dfae9a25aa4a940067e34bf4cebde5926</td>
      <td>[8, 3f2b2dfa, 9a25aa4a940067, 34bf4c, bd, 5926]</td>
    </tr>
  </tbody>
</table>
</div>



### DataFrame and SQL APIs

When you registered the UDF, it was named `manualSplitSQLUDF` for access in the SQL API. This gives us the same access to the UDF you had in the python DataFrames API.

Register `randomDF` to access it within SQL.


```python
randomDF.createOrReplaceTempView("randomTable")
```

Now switch to the SQL API and use the same UDF.


```python
display(
    spark.sql("""
SELECT id,
  hash,
  manualSplitSQLUDF(hash) as augmented_col
FROM
  randomTable""")
)
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
      <th>hash</th>
      <th>augmented_col</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>6f65a37773e1a173aefe023667fe23442efaf0e5</td>
      <td>[6f65a37773, 1a173a, f, 023667f, 23442, faf0, 5]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>3c5f154011aa08151d2cc5b458a48b33ab656c67</td>
      <td>[3c5f154011aa08151d2cc5b458a48b33ab656c67]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>28fc878d1f1942bd718cefa60424d9e18e8756db</td>
      <td>[28fc878d1f1942bd718c, fa60424d9, 18, 8756db]</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>9c54ebb1c11c92d4ce7c2d52a15d21c337e0ced5</td>
      <td>[9c54, bb1c11c92d4c, 7c2d52a15d21c337, 0c, d5]</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>46500976da0a25029a93419acd36935d06c86e95</td>
      <td>[46500976da0a25029a93419acd36935d06c86, 95]</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>9d8e41374ef6637e274e4b5487a6428b72b5fb2a</td>
      <td>[9d8, 41374, f6637, 274, 4b5487a6428b72b5fb2a]</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>29d9fa515d557568abc85d08d1217b6b21c4841d</td>
      <td>[29d9fa515d557568abc85d08d1217b6b21c4841d]</td>
    </tr>
    <tr>
      <th>7</th>
      <td>8</td>
      <td>672da7735c57d6707a8307bd18b1fd4dc04821ee</td>
      <td>[672da7735c57d6707a8307bd18b1fd4dc04821, , ]</td>
    </tr>
    <tr>
      <th>8</th>
      <td>9</td>
      <td>e6fb1c43359cff2d70014e776bd47565bdbf40a2</td>
      <td>[, 6fb1c43359cff2d70014, 776bd47565bdbf40a2]</td>
    </tr>
    <tr>
      <th>9</th>
      <td>10</td>
      <td>8e3f2b2dfae9a25aa4a940067e34bf4cebde5926</td>
      <td>[8, 3f2b2dfa, 9a25aa4a940067, 34bf4c, bd, 5926]</td>
    </tr>
  </tbody>
</table>
</div>



This is an easy way to generalize UDFs, allowing teams to share their code.

### Performance Trade-offs

The performance of custom UDFs normally trail far behind built-in functions.  Take a look at this other example to compare built-in functions to custom UDFs.

Create a large DataFrame of random values, cache the result in order to keep the DataFrame in memory, and perform a `.count()` to trigger the cache to take effect.


```python
from pyspark.sql.functions import col, rand

randomFloatsDF = (spark.range(0, 100 * 1000 * 1000)
  .withColumn("id", (col("id") / 1000).cast("integer"))
  .withColumn("random_float", rand())
)

randomFloatsDF.cache()
randomFloatsDF.count()

display(randomFloatsDF)
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
      <th>random_float</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>0.182282</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>0.104614</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0</td>
      <td>0.610698</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0</td>
      <td>0.401528</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0</td>
      <td>0.395210</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0</td>
      <td>0.473078</td>
    </tr>
    <tr>
      <th>6</th>
      <td>0</td>
      <td>0.049408</td>
    </tr>
    <tr>
      <th>7</th>
      <td>0</td>
      <td>0.198057</td>
    </tr>
    <tr>
      <th>8</th>
      <td>0</td>
      <td>0.212611</td>
    </tr>
    <tr>
      <th>9</th>
      <td>0</td>
      <td>0.992668</td>
    </tr>
  </tbody>
</table>
</div>



Register a new UDF that increments a column by 1.  Here, use a lambda instead of a function.


```python
from pyspark.sql.types import FloatType
  
plusOneUDF = spark.udf.register("plusOneUDF", lambda x: x + 1, FloatType())
```

Compare the results using the `%timeit` function.  Run it a few times and examine the results.


```python
%timeit randomFloatsDF.withColumn("incremented_float", plusOneUDF("random_float")).count()
```

    12.3 s ± 3.21 s per loop (mean ± std. dev. of 7 runs, 1 loop each)



```python
%timeit randomFloatsDF.withColumn("incremented_float", col("random_float") + 1).count()
```

    13.5 s ± 355 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)


Which was faster, the UDF or the built-in functionality?  By how much?  This can differ based upon whether you work through this course in Scala (which is much faster) or Python. Please go ahead and try this on Zeppelin using scala.

## Exercise 1: Converting IP Addresses to Decimals

Write a UDF that translates an IPv4 address string (e.g. `123.123.123.123`) into a numeric value.

### Step 1: Create a Function

IP addresses pose challenges for efficient database lookups.  One way of dealing with this is to store an IP address in numerical form.  Write the function `IPConvert` that satisfies the following:

Input: IP address as a string (e.g. `123.123.123.123`)  
Output: an integer representation of the IP address (e.g. `2071690107`)

If the input string is `1.2.3.4`, think of it like `A.B.C.D` where A is 1, B is 2, etc. Solve this with the following steps:

&nbsp;&nbsp;&nbsp; (A x 256^3) + (B x 256^2) + (C x 256) + D <br>
&nbsp;&nbsp;&nbsp; (1 x 256^3) + (2 x 256^2) + (3 x 256) + 4 <br>
&nbsp;&nbsp;&nbsp; 116777216 + 131072 + 768 + 4 <br>
&nbsp;&nbsp;&nbsp; 16909060

Make a function to implement this.



```python
# TODO
def IPConvert(ip):
    a, b, c, d = ip.split(".")
    print(a, b, c, d)
    return ((int(a) * (256**3)) + (int(b) * (256**2)) + (int(c) * (256)) + int(d))
IPConvert('1.2.3.4')
```

    1 2 3 4





    16909060




```python
# TEST - Run this cell to test your solution
dfTest("ET2-P-03-01-01", 16909060, IPConvert("1.2.3.4"))
dfTest("ET2-P-03-01-02", 168430090, IPConvert("10.10.10.10"))
dfTest("ET2-P-03-01-03", 386744599, IPConvert("23.13.65.23"))

print("Tests passed!")
```

    1 2 3 4
    10 10 10 10
    23 13 65 23
    Tests passed!


### Step 2: Register a UDF

Register your function as `IPConvertUDF`.  Be sure to use `LongType` as your output type.


```python
# TODO
from pyspark.sql.types import IntegerType

IPConvertUDF = spark.udf.register('IPConvertSQLUDF', IPConvert, IntegerType())
```


```python
# TEST - Run this cell to test your solution
testDF = spark.createDataFrame((
  ("1.2.3.4", ),
  ("10.10.10.10", ),
  ("23.13.65.23", )
), ("ip",))
result = [i[0] for i in testDF.select(IPConvertUDF("ip")).collect()]

dfTest("ET2-P-03-02-01", 16909060, result[0])
dfTest("ET2-P-03-02-02", 168430090, result[1])
dfTest("ET2-P-03-02-03", 386744599, result[2])

print("Tests passed!")
```

    Tests passed!


### Step 3: Apply the UDF

Apply the UDF on the `IP` column of the DataFrame created below, creating the new column `parsedIP`.


```python
IPDF = spark.createDataFrame([["123.123.123.123"], ["1.2.3.4"], ["127.0.0.0"]], ['ip'])
```


```python
# TODO
IPDFWithParsedIP = IPDF.withColumn('parsedIP', IPConvertUDF(F.col('ip')))
```


```python
display(IPDFWithParsedIP)
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
      <th>parsedIP</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>123.123.123.123</td>
      <td>2071690107</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1.2.3.4</td>
      <td>16909060</td>
    </tr>
    <tr>
      <th>2</th>
      <td>127.0.0.0</td>
      <td>2130706432</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
result2 = [i[1] for i in IPDFWithParsedIP.collect()]
    
dfTest("ET2-P-03-03-01", 2071690107, result2[0])
dfTest("ET2-P-03-03-02", 16909060, result2[1])
dfTest("ET2-P-03-03-03", 2130706432, result2[2])

print("Tests passed!")
```

    Tests passed!


## Review
**Question:** What are the performance trade-offs between UDFs and built-in functions?  When should I use each?  
**Answer:** Built-in functions are normally faster than UDFs and should be used when possible.  UDFs should be used when specific use cases arise that aren't addressed by built-in functions.

**Question:** How can I use UDFs?  
**Answer:** UDFs can be used in any Spark API. They can be registered for use in SQL and can otherwise be used in Scala, Python, R, and Java.

**Question:** Why are built-in functions faster?  
**Answer:** Reasons include:
* The catalyst optimizer knows how to optimize built-in functions
* They are written in highly optimized Scala
* There is no serialization cost at the time of running a built-in function

**Question:** Can UDFs have multiple column inputs and outputs?  
**Answer:** Yes, UDFs can have multiple column inputs and multiple complex outputs. This is covered in the following lesson.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
