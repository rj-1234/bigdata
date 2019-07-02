
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Applying Schemas to JSON Data

Apache Spark&trade; provide a number of ways to project structure onto semi-structured data allowing for quick and easy access.
## In this lesson you:
* Infer the schema from JSON files
* Create and use a user-defined schema with primitive data types
* Use non-primitive data types such as `ArrayType` and `MapType` in a schema



### Schemas

Schemas are at the heart of data structures in Spark.
**A schema describes the structure of your data by naming columns and declaring the type of data in that column.** 
Rigorously enforcing schemas leads to significant performance optimizations and reliability of code.

Why is open source Spark so fast? While there are many reasons for these performance improvements, two key reasons are:<br><br>
* First and foremost, Spark runs first in memory rather than reading and writing to disk. 
* Second, using DataFrames allows Spark to optimize the execution of your queries because it knows what your data looks like.

Two pillars of computer science education are data structures, the organization and storage of data and algorithms, and the computational procedures on that data.  A rigorous understanding of computer science involves both of these domains. When you apply the most relevant data structures, the algorithms that carry out the computation become significantly more eloquent.

In the road map for ETL, this is the **Apply Schema** step:

<img src="../../resources/ETL-Process-2.png" style="border: 1px solid #aaa; border-radius: 10px 10px 10px 10px; box-shadow: 5px 5px 5px #aaa"/>

### Schemas with Semi-Structured JSON Data

**Tabular data**, such as that found in CSV files or relational databases, has a formal structure where each observation, or row, of the data has a value (even if it's a NULL value) for each feature, or column, in the data set.  

**Semi-structured data** does not need to conform to a formal data model. Instead, a given feature may appear zero, once, or many times for a given observation.  

Semi-structured data storage works well with hierarchical data and with schemas that may evolve over time.  One of the most common forms of semi-structured data is JSON data, which consists of attribute-value pairs.


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
            setAppName("pyspark_etl_02_applying_schemas_to_json").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_02_applying_schemas_to_json").\
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
        


    Stopped a SparkSession


Print the first few lines of a JSON file holding ZIP Code data.


```python
!aws s3 ls s3://data.intellinum.co/bootcamp/common/zips.json
```

    2019-06-05 10:09:11    3182409 zips.json


### Schema Inference

Import data as a DataFrame and view its schema with the `printSchema()` DataFrame method.


```python
zipsDF = spark.read.json("s3a://data.intellinum.co/bootcamp/common/zips.json")
zipsDF.printSchema()
```

    root
     |-- _id: string (nullable = true)
     |-- city: string (nullable = true)
     |-- loc: array (nullable = true)
     |    |-- element: double (containsNull = true)
     |-- pop: long (nullable = true)
     |-- state: string (nullable = true)
    


Store the schema as an object by calling `.schema` on a DataFrame. Schemas consist of a `StructType`, which is a collection of `StructField`s.  Each `StructField` gives a name and a type for a given field in the data.


```python
zipsSchema = zipsDF.schema
print(type(zipsSchema))

[field for field in zipsSchema]
```

    <class 'pyspark.sql.types.StructType'>





    [StructField(_id,StringType,true),
     StructField(city,StringType,true),
     StructField(loc,ArrayType(DoubleType,true),true),
     StructField(pop,LongType,true),
     StructField(state,StringType,true)]



### User-Defined Schemas

Spark infers schemas from the data, as detailed in the example above.  Challenges with inferred schemas include:  
<br>
* Schema inference means Spark scans all of your data, creating an extra job, which can affect performance
* Consider providing alternative data types (for example, change a `Long` to a `Integer`)
* Consider throwing out certain fields in the data, to read only the data of interest

To define schemas, build a `StructType` composed of `StructField`s.

Import the necessary types from the `types` module. Build a `StructType`, which takes a list of `StructField`s.  Each `StructField` takes three arguments: the name of the field, the type of data in it, and a `Boolean` for whether this field can be `Null`.


```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

zipsSchema2 = StructType([
  StructField("city", StringType(), True), 
  StructField("pop", IntegerType(), True) 
])
```

Apply the schema using the `.schema` method. This `read` returns only  the columns specified in the schema and changes the column `pop` from `LongType` (which was inferred above) to `IntegerType`.

A `LongType` is an 8-byte integer ranging up to 9,223,372,036,854,775,807 while `IntegerType` is a 4-byte integer ranging up to 2,147,483,647.  Since no American city has over two billion people, `IntegerType` is sufficient.


```python
zipsDF2 = (spark.read
  .schema(zipsSchema2)
  .json("s3a://data.intellinum.co/bootcamp/common/zips.json")
)

display(zipsDF2)
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
      <th>city</th>
      <th>pop</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AGAWAM</td>
      <td>15338</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CUSHMAN</td>
      <td>36963</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BARRE</td>
      <td>4546</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BELCHERTOWN</td>
      <td>10579</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BLANDFORD</td>
      <td>1240</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BRIMFIELD</td>
      <td>3706</td>
    </tr>
    <tr>
      <th>6</th>
      <td>CHESTER</td>
      <td>1688</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CHESTERFIELD</td>
      <td>177</td>
    </tr>
    <tr>
      <th>8</th>
      <td>CHICOPEE</td>
      <td>23396</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHICOPEE</td>
      <td>31495</td>
    </tr>
  </tbody>
</table>
</div>



### Primitive and Non-primitive Types

The Spark [`types` package](http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql.types) provides the building blocks for constructing schemas.

A primitive type contains the data itself.  The most common primitive types include:

| Numeric | General | Time |
|-----|-----|
| `FloatType` | `StringType` | `TimestampType` | 
| `IntegerType` | `BooleanType` | `DateType` | 
| `DoubleType` | `NullType` | |
| `LongType` | | |
| `ShortType` |  | |

Non-primitive types are sometimes called reference variables or composite types.  Technically, non-primitive types contain references to memory locations and not the data itself.  Non-primitive types are the composite of a number of primitive types such as an Array of the primitive type `Integer`.

The two most common composite types are `ArrayType` and `MapType`. These types allow for a given field to contain an arbitrary number of elements in either an Array/List or Map/Dictionary form.

See the [Spark documentation](http://spark.apache.org/docs/latest/sql-programming-guide.html#data-types) for a complete picture of types in Spark.

The ZIP Code dataset contains an array with the latitude and longitude of the cities.  Use an `ArrayType`, which takes the primitive type of its elements as an argument.


```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType

zipsSchema3 = StructType([
  StructField("city", StringType(), True), 
  StructField("loc", 
    ArrayType(FloatType(), True), True),
  StructField("pop", IntegerType(), True)
])
```

Apply the schema using the `.schema()` method and observe the results.  Expand the array values in the column `loc` to explore further.


```python
zipsDF3 = (spark.read
  .schema(zipsSchema3)
  .json("s3a://data.intellinum.co/bootcamp/common/zips.json")
)
display(zipsDF3)
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
      <th>city</th>
      <th>loc</th>
      <th>pop</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>AGAWAM</td>
      <td>[-72.62274169921875, 42.07020568847656]</td>
      <td>15338</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CUSHMAN</td>
      <td>[-72.5156478881836, 42.377017974853516]</td>
      <td>36963</td>
    </tr>
    <tr>
      <th>2</th>
      <td>BARRE</td>
      <td>[-72.10835266113281, 42.409698486328125]</td>
      <td>4546</td>
    </tr>
    <tr>
      <th>3</th>
      <td>BELCHERTOWN</td>
      <td>[-72.41094970703125, 42.27510452270508]</td>
      <td>10579</td>
    </tr>
    <tr>
      <th>4</th>
      <td>BLANDFORD</td>
      <td>[-72.93611145019531, 42.18294906616211]</td>
      <td>1240</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BRIMFIELD</td>
      <td>[-72.1884536743164, 42.11654281616211]</td>
      <td>3706</td>
    </tr>
    <tr>
      <th>6</th>
      <td>CHESTER</td>
      <td>[-72.98876190185547, 42.279422760009766]</td>
      <td>1688</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CHESTERFIELD</td>
      <td>[-72.83330535888672, 42.38167190551758]</td>
      <td>177</td>
    </tr>
    <tr>
      <th>8</th>
      <td>CHICOPEE</td>
      <td>[-72.60796356201172, 42.162044525146484]</td>
      <td>23396</td>
    </tr>
    <tr>
      <th>9</th>
      <td>CHICOPEE</td>
      <td>[-72.57614135742188, 42.17644119262695]</td>
      <td>31495</td>
    </tr>
  </tbody>
</table>
</div>



## Exercise 1: Exploring JSON Data

<a href="https://archive.ics.uci.edu/ml/datasets/UbiqLog+(smartphone+lifelogging)">Smartphone data from UCI Machine Learning Repository</a> is available under `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI`. This is log data from the open source project [Ubiqlog](https://github.com/Rezar/Ubiqlog).

Import this data and define your own schema.

### Step 1: Import the Data

Import data from `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*`. (This is the log files from a given user.)

Look at the head of one file from the data set using AWS CLI.  Use `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log_1-6-2014.txt`.

**Hint:** You can google `AWS S3 head`.


```python
#TODO
!aws s3api head-object --bucket data.intellinum.co --key bootcamp/common/UbiqLog4UCI/14_F/log_1-6-2014.txt
```

    {
        "AcceptRanges": "bytes",
        "LastModified": "Wed, 05 Jun 2019 10:14:36 GMT",
        "ContentLength": 20044,
        "ETag": "\"fa1e90a04bdc00b9a2df2754444454b5\"",
        "ContentType": "text/plain",
        "Metadata": {}
    }


Read the data and save it to `smartphoneDF`. Read the logs using a `*` in your path like `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*`.


```python
#TODO
smartphoneDF = spark.read.json("s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*")
```


```python
from pyspark.sql.functions import desc

display(smartphoneDF.orderBy(desc("Application")))
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
      <th>Application</th>
      <th>Bluetooth</th>
      <th>Call</th>
      <th>Location</th>
      <th>SMS</th>
      <th>WiFi</th>
      <th>_corrupt_record</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(12-9-2013 21:30:02, com.android.settings, 12-...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>(12-9-2013 21:29:58, com.farsitel.bazaar, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>(12-9-2013 21:25:17, com.android.packageinstal...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>(12-9-2013 21:25:11, com.farsitel.bazaar, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>(12-9-2013 21:21:00, com.android.browser, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>5</th>
      <td>(12-9-2013 21:20:46, com.farsitel.bazaar, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>6</th>
      <td>(12-9-2013 21:19:56, com.google.android.gms.ui...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>7</th>
      <td>(12-9-2013 21:19:40, com.google.process.locati...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>8</th>
      <td>(12-9-2013 21:19:30, com.farsitel.bazaar, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>9</th>
      <td>(12-9-2013 21:17:50, com.android.browser, 12-9...</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
from pyspark.sql.functions import desc

cols = set(smartphoneDF.columns)
sample = smartphoneDF.orderBy(desc("Application")).first()[0][0]

dfTest("ET1-P-05-01-01", 25372, smartphoneDF.count())
dfTest("ET1-P-05-01-02", '12-9-2013 21:30:02', sample)

dfTest("ET1-P-05-01-03", True, "Location" in cols)
dfTest("ET1-P-05-01-04", True, "SMS" in cols)
dfTest("ET1-P-05-01-05", True, "WiFi" in cols)
dfTest("ET1-P-05-01-06", True, "_corrupt_record" in cols)
dfTest("ET1-P-05-01-07", True, "Application" in cols)
dfTest("ET1-P-05-01-08", True, "Call" in cols)
dfTest("ET1-P-05-01-09", True, "Bluetooth" in cols)

print("Tests passed!")
```

    Tests passed!


### Step 2: Explore the Inferred Schema

Print the schema to get a sense for the data.


```python
# TODO
smartphoneDF.printSchema()
```

    root
     |-- Application: struct (nullable = true)
     |    |-- End: string (nullable = true)
     |    |-- ProcessName: string (nullable = true)
     |    |-- Start: string (nullable = true)
     |-- Bluetooth: struct (nullable = true)
     |    |-- address: string (nullable = true)
     |    |-- bond status: string (nullable = true)
     |    |-- name: string (nullable = true)
     |    |-- time: string (nullable = true)
     |-- Call: struct (nullable = true)
     |    |-- Duration: string (nullable = true)
     |    |-- Number: string (nullable = true)
     |    |-- Time: string (nullable = true)
     |    |-- Type: string (nullable = true)
     |-- Location: struct (nullable = true)
     |    |-- Accuracy: string (nullable = true)
     |    |-- Altitude: string (nullable = true)
     |    |-- Latitude: string (nullable = true)
     |    |-- Longtitude: string (nullable = true)
     |    |-- Provider: string (nullable = true)
     |    |-- time: string (nullable = true)
     |-- SMS: struct (nullable = true)
     |    |-- Address: string (nullable = true)
     |    |-- Type: string (nullable = true)
     |    |-- body: string (nullable = true)
     |    |-- date: string (nullable = true)
     |    |-- metadata: struct (nullable = true)
     |    |    |-- name: string (nullable = true)
     |    |-- type: string (nullable = true)
     |-- WiFi: struct (nullable = true)
     |    |-- BSSID: string (nullable = true)
     |    |-- SSID: string (nullable = true)
     |    |-- capabilities: string (nullable = true)
     |    |-- frequency: string (nullable = true)
     |    |-- level: string (nullable = true)
     |    |-- time: string (nullable = true)
     |-- _corrupt_record: string (nullable = true)
    



```python
smartphoneSchema = smartphoneDF.schema
print(type(smartphoneSchema))

[field for field in smartphoneSchema]
```

    <class 'pyspark.sql.types.StructType'>





    [StructField(Application,StructType(List(StructField(End,StringType,true),StructField(ProcessName,StringType,true),StructField(Start,StringType,true))),true),
     StructField(Bluetooth,StructType(List(StructField(address,StringType,true),StructField(bond status,StringType,true),StructField(name,StringType,true),StructField(time,StringType,true))),true),
     StructField(Call,StructType(List(StructField(Duration,StringType,true),StructField(Number,StringType,true),StructField(Time,StringType,true),StructField(Type,StringType,true))),true),
     StructField(Location,StructType(List(StructField(Accuracy,StringType,true),StructField(Altitude,StringType,true),StructField(Latitude,StringType,true),StructField(Longtitude,StringType,true),StructField(Provider,StringType,true),StructField(time,StringType,true))),true),
     StructField(SMS,StructType(List(StructField(Address,StringType,true),StructField(Type,StringType,true),StructField(body,StringType,true),StructField(date,StringType,true),StructField(metadata,StructType(List(StructField(name,StringType,true))),true),StructField(type,StringType,true))),true),
     StructField(WiFi,StructType(List(StructField(BSSID,StringType,true),StructField(SSID,StringType,true),StructField(capabilities,StringType,true),StructField(frequency,StringType,true),StructField(level,StringType,true),StructField(time,StringType,true))),true),
     StructField(_corrupt_record,StringType,true)]



The schema shows:  

* Six categories of tracked data 
* Nested data structures
* A field showing corrupt records

## Exercise 2: Creating a User Defined Schema

### Step 1: Set Up Your workflow

Often the hardest part of a coding challenge is setting up a workflow to get continuous feedback on what you develop.

Start with the import statements you need, including functions from two main packages:

| Package | Function |
|---------|---------|
| `pyspark.sql.types` | `StructType`, `StructField`, `StringType` |
| `pyspark.sql.functions` | `col` |


```python
# TODO
from pyspark.sql.types import StructType, StructField, StringType
```

The **SMS** field needs to be parsed. Create a placeholder schema called `schema` that's a `StructType` with one `StructField` named **SMS** of type `StringType`. This imports the entire attribute (even though it contains nested entities) as a String.  

This is a way to get a sense for what's in the data and make a progressively more complex schema.


```python
# TODO
schema = StructType([
    StructField("SMS" , StringType(), True)
])
```


```python
# TEST - Run this cell to test your solution
fields = schema.fields
print(fields)
dfTest("ET1-P-05-02-01", 1, len(fields))
dfTest("ET1-P-05-02-02", 'SMS', fields[0].name)

print("Tests passed!")
```

    [StructField(SMS,StringType,true)]
    Tests passed!


Apply the schema to the data and save the result as `SMSDF`. This closes the loop on which to iterate and develop an increasingly complex schema. The path to the data is `s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*`. 

Include only records where the column `SMS` is not `Null`.


```python
# TODO
schema = StructType([
  StructField("SMS", StringType(), False)
])

SMSDF = (spark
         .read
         .schema(schema)
         .json('s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*')).filter("SMS != 'None' ")
```


```python
SMSDF.printSchema()
```

    root
     |-- SMS: string (nullable = true)
    



```python
SMSDF.limit(5).show()
```

    +--------------------+
    |                 SMS|
    +--------------------+
    |{"Address":"+9821...|
    |{"Address":"+9850...|
    |{"Address":"+9821...|
    |{"Address":"+9893...|
    |{"Address":"+9821...|
    +--------------------+
    



```python
# TEST - Run this cell to test your solution
cols = SMSDF.columns

dfTest("ET1-P-05-03-01", 1147, SMSDF.count())
dfTest("ET1-P-05-03-02", ['SMS'], cols)

print("Tests passed!")
```

    Tests passed!


### Step 2: Create the Full Schema for SMS

Define the Schema for the following fields in the `StructType` `SMS` and name it `schema2`.  Apply it to a new DataFrame `SMSDF2`:  
<br>
* `Address`
* `date`
* `metadata`
 - `name`
 
Note there's `Type` and `type`, which appears to be redundant data.  


```python
# TODO
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType

schema2 = StructType([
    StructField("SMS", StructType([
        StructField("Address" , StringType(), True),
        StructField("date", DateType(), True),
        StructField("metadata", StructType([
            StructField("name", StringType(), True)
        ]), True)
    ]), True)
])
```


```python
print([field for field in schema2])
```

    [StructField(SMS,StructType(List(StructField(Address,StringType,true),StructField(date,DateType,true),StructField(metadata,StructType(List(StructField(name,StringType,true))),true))),true)]



```python
readSMSDF2 = (spark
          .read
          .schema(schema2)
          .json('s3://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*'))
```


```python
SMSDF2 = readSMSDF2.filter(readSMSDF2.SMS.isNotNull())
```


```python
SMSDF2.printSchema()
```

    root
     |-- SMS: struct (nullable = true)
     |    |-- Address: string (nullable = true)
     |    |-- date: date (nullable = true)
     |    |-- metadata: struct (nullable = true)
     |    |    |-- name: string (nullable = true)
    



```python
display(SMSDF2)
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
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>(+98214428####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>1</th>
      <td>(+985000406500####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>2</th>
      <td>(+98214428####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>3</th>
      <td>(+98939283####, 0007-03-05, (bahram,))</td>
    </tr>
    <tr>
      <th>4</th>
      <td>(+98214428####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>5</th>
      <td>(+98939283####, 0007-03-05, (bahram,))</td>
    </tr>
    <tr>
      <th>6</th>
      <td>(+98935566####, 0007-04-04, (u Kh sevda,))</td>
    </tr>
    <tr>
      <th>7</th>
      <td>(+98214428####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>8</th>
      <td>(+981000721670####, 0007-04-04, (,))</td>
    </tr>
    <tr>
      <th>9</th>
      <td>(+98935566####, 0007-04-04, (u Kh sevda,))</td>
    </tr>
  </tbody>
</table>
</div>




```python
# TEST - Run this cell to test your solution
cols = SMSDF2.columns
schemaJson = SMSDF2.schema.json()

dfTest("ET1-P-05-04-01", 1147, SMSDF2.count())
dfTest("ET1-P-05-04-02", ['SMS'], cols)
dfTest("ET1-P-05-04-03", True, 'Address' in schemaJson and 'date' in schemaJson)

print("Tests passed!")
```

    Tests passed!


### Step 3: Compare Solution Performance

Compare the dafault schema inference to applying a user defined schema using the `%timeit` function.  Which completed faster?  Which triggered more jobs?  Why?


```python
%timeit SMSDF = spark.read.schema(schema2).json("s3a://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*").count()
```

    3.41 s ± 334 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)



```python
%timeit SMSDF = spark.read.json("s3a://data.intellinum.co/bootcamp/common/UbiqLog4UCI/14_F/log*").count()

```

    5.66 s ± 167 ms per loop (mean ± std. dev. of 7 runs, 1 loop each)


Providing a schema increases performance two to three times, depending on the size of the cluster used. Since Spark doesn't infer the schema, it doesn't have to read through all of the data. This is also why there are fewer jobs when a schema is provided: Spark doesn't need one job for each partition of the data to infer the schema.

## Review

**Question:** What are two ways to attain a schema from data?  
**Answer:** Allow Spark to infer a schema from your data or provide a user defined schema. Schema inference is the recommended first step; however, you can customize this schema to your use case with a user defined schema.

**Question:** Why should you define your own schema?  
**Answer:** Benefits of user defined schemas include:
* Avoiding the extra scan of your data needed to infer the schema
* Providing alternative data types
* Parsing only the fields you need

**Question:** Why is JSON a common format in big data pipelines?  
**Answer:** Semi-structured data works well with hierarchical data and where schemas need to evolve over time.  It also easily contains composite data types such as arrays and maps.

**Question:** By default, how are corrupt records dealt with using `spark.read.json()`?  
**Answer:** They appear in a column called `_corrupt_record`.  These are the records that Spark can't read (e.g. when characters are missing from a JSON string).

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
