
<div style="text-align: center; line-height: 0; padding-top: 9px;">
  <img src="../../resources/logo.png" alt="Intellinum Bootcamp" style="width: 600px; height: 163px">
</div>

# Advanced User Defined Functions

Apache Spark&trade; allows you to create your own User Defined Functions (UDFs) specific to the needs of your data.

## In this lesson you:
* Apply UDFs with a multiple DataFrame column inputs
* Apply UDFs that return complex types
* Write vectorized UDFs using Python


### Complex Transformations
 
UDFs provide custom, generalizable code that you can apply to ETL workloads when Spark's built-in functions won't suffice.  
In the last lesson we covered a simple version of this: UDFs that take a single DataFrame column input and return a primitive value. Often a more advanced solution is needed.

UDFs can take multiple column inputs. While UDFs cannot return multiple columns, they can return complex, named types that are easily accessible. This approach is especially helpful in ETL workloads that need to clean complex and challenging data structures.

Another other option is the new vectorized, or pandas, UDFs available in Spark 2.3. These allow for more performant UDFs written in Python.<br><br>

<div><img src="../../resources/pandas-udfs.png" style="height: 400px; margin: 20px"/></div>

### UDFs with Multiple Columns

To begin making more complex UDFs, start by using multiple column inputs.  This is as simple as adding extra inputs to the function or lambda you convert to the UDF.

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
            setAppName("pyspark_etl_08-advanced-udf").\
            setMaster('local[*]').\
            set('spark.driver.maxResultSize', '0').\
            set('spark.jars', '../../libs/mysql-connector-java-5.1.45-bin.jar').\
            set('spark.jars.packages','net.java.dev.jets3t:jets3t:0.9.0,com.google.guava:guava:16.0.1,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1')
else:
    os.environ["PYSPARK_PYTHON"] = "./MN/pyspark24_py36/bin/python"
    conf = SparkConf().\
            setAppName("pyspark_etl_08-advanced-udf-rajeev").\
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


Write a basic function that combines two columns.


```python
def manual_add(x, y):
    return x + y

manual_add(1, 2)
```




    3



Register the function as a UDF by binding it with a Python variable, adding a name to access it in the SQL API and giving it a return type.


```python
from pyspark.sql.types import IntegerType

manualAddPythonUDF = spark.udf.register("manualAddSQLUDF", manual_add, IntegerType())
```

Create a dummy DataFrame to apply the UDF.


```python
integerDF = (spark.createDataFrame([
  (1, 2),
  (3, 4),
  (5, 6)
], ["col1", "col2"]))

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
      <th>col1</th>
      <th>col2</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



Apply the UDF to your DataFrame.


```python
integerAddDF = integerDF.select("*", manualAddPythonUDF("col1", "col2").alias("sum"))

display(integerAddDF)
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
      <th>col1</th>
      <th>col2</th>
      <th>sum</th>
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
      <td>3</td>
      <td>4</td>
      <td>7</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5</td>
      <td>6</td>
      <td>11</td>
    </tr>
  </tbody>
</table>
</div>



### UDFs with Complex Output

Complex outputs are helpful when you need to return multiple values from your UDF. The UDF design pattern involves returning a single column to drill down into, to pull out the desired data.

Start by determining the desired output.  This will look like a schema with a high level `StructType` with numerous `StructFields`.

For a refresher on this, see the lesson **Applying Schemas to JSON Data** in <a href="./02-applying-schemas-to-JSON.ipynb" target="_blank">ETL Part 1 course</a>.


```python
from pyspark.sql.types import FloatType, StructType, StructField

mathOperationsSchema = StructType([
  StructField("sum", FloatType(), True), 
  StructField("multiplication", FloatType(), True), 
  StructField("division", FloatType(), True) 
])
```

Create a function that returns a tuple of your desired output.


```python
def manual_math(x, y):
    return (float(x + y), float(x * y), x / float(y))

manual_math(1, 2)
```




    (3.0, 2.0, 0.5)



Register your function as a UDF and apply it.  In this case, your return type is the schema you created.


```python
manualMathPythonUDF = spark.udf.register("manualMathSQLUDF", manual_math, mathOperationsSchema)

display(integerDF.select("*", manualMathPythonUDF("col1", "col2").alias("sum")))
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
      <th>col1</th>
      <th>col2</th>
      <th>sum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2</td>
      <td>(3.0, 2.0, 0.5)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>4</td>
      <td>(7.0, 12.0, 0.75)</td>
    </tr>
    <tr>
      <th>2</th>
      <td>5</td>
      <td>6</td>
      <td>(11.0, 30.0, 0.8333333134651184)</td>
    </tr>
  </tbody>
</table>
</div>



### Vectorized UDFs in Python

Starting in Spark 2.3, vectorized UDFs can be written in Python called Pandas UDFs.  This alleviates some of the serialization and invocation overhead of conventional Python UDFs.  While there are a number of types of these UDFs, this walk-through focuses on scalar UDFs. This is an ideal solution for Data Scientists needing performant UDFs written in Python.

:NOTE: Your cluster will need to run Spark 2.3 in order to execute the following code.

Use the decorator syntax to designate a Pandas UDF.  The input and outputs are both Pandas series of doubles.


```python
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf('double', PandasUDFType.SCALAR)
def pandas_plus_one(v):
    return v + 1
```

Create a DataFrame to apply the UDF.


```python
from pyspark.sql.functions import col, rand

df = spark.range(0, 10 * 1000 * 1000)

display(df)
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
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
    </tr>
    <tr>
      <th>8</th>
      <td>8</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
    </tr>
  </tbody>
</table>
</div>



Apply the UDF


```python
display(df.withColumn('id_transformed', pandas_plus_one("id")))
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
      <th>id_transformed</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>3.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>4.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>5.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>6.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
      <td>7.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>8.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>8</td>
      <td>9.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>9</td>
      <td>10.0</td>
    </tr>
  </tbody>
</table>
</div>



## Exercise 1: Multiple Column Inputs to Complex Type

Given a DataFrame of weather in various units, write a UDF that translates a column for temperature and a column for units into a complex type for temperature in three units:<br><br>

* fahrenheit
* celsius
* kelvin

### Step 1: Import and Explore the Data

Import the data sitting in `s3a://data.intellinum.co/bootcamp/common/weather/StationData/stationData.parquet` and save it to `weatherDF`.


```python
# TODO
path = "s3a://data.intellinum.co/bootcamp/common/weather/StationData/stationData.parquet"
weatherDF = spark.read.parquet(path)
```


```python
display(weatherDF)
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
      <th>NAME</th>
      <th>STATION</th>
      <th>LATITUDE</th>
      <th>LONGITUDE</th>
      <th>ELEVATION</th>
      <th>DATE</th>
      <th>UNIT</th>
      <th>TAVG</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>HAYWARD AIR TERMINAL, CA US</td>
      <td>USW00093228</td>
      <td>37.654202</td>
      <td>-122.114998</td>
      <td>13.100000</td>
      <td>2018-05-27</td>
      <td>F</td>
      <td>61.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BIG ROCK CALIFORNIA, CA US</td>
      <td>USR0000CBIR</td>
      <td>38.039398</td>
      <td>-122.570000</td>
      <td>457.200012</td>
      <td>2018-01-05</td>
      <td>C</td>
      <td>11.7</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SAN FRANCISCO INTERNATIONAL AIRPORT, CA US</td>
      <td>USW00023234</td>
      <td>37.619701</td>
      <td>-122.364700</td>
      <td>2.400000</td>
      <td>2018-02-24</td>
      <td>C</td>
      <td>8.3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LAS TRAMPAS CALIFORNIA, CA US</td>
      <td>USR0000CTRA</td>
      <td>37.833900</td>
      <td>-122.066902</td>
      <td>536.400024</td>
      <td>2018-03-26</td>
      <td>C</td>
      <td>9.4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>HOUSTON INTERCONTINENTAL AIRPORT, TX US</td>
      <td>USW00012960</td>
      <td>29.980000</td>
      <td>-95.360001</td>
      <td>29.000000</td>
      <td>2018-05-25</td>
      <td>F</td>
      <td>80.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BIG ROCK CALIFORNIA, CA US</td>
      <td>USR0000CBIR</td>
      <td>38.039398</td>
      <td>-122.570000</td>
      <td>457.200012</td>
      <td>2018-05-16</td>
      <td>C</td>
      <td>11.1</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BLACK DIAMOND CALIFORNIA, CA US</td>
      <td>USR0000CBKD</td>
      <td>37.950001</td>
      <td>-121.884399</td>
      <td>487.700012</td>
      <td>2018-05-25</td>
      <td>C</td>
      <td>10.6</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LAS TRAMPAS CALIFORNIA, CA US</td>
      <td>USR0000CTRA</td>
      <td>37.833900</td>
      <td>-122.066902</td>
      <td>536.400024</td>
      <td>2018-05-21</td>
      <td>C</td>
      <td>11.7</td>
    </tr>
    <tr>
      <th>8</th>
      <td>WOODACRE CALIFORNIA, CA US</td>
      <td>USR0000CWOO</td>
      <td>37.990601</td>
      <td>-122.644699</td>
      <td>426.700012</td>
      <td>2018-05-26</td>
      <td>F</td>
      <td>53.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BRIONES CALIFORNIA, CA US</td>
      <td>USR0000CBRI</td>
      <td>37.944199</td>
      <td>-122.117798</td>
      <td>442.000000</td>
      <td>2018-04-08</td>
      <td>F</td>
      <td>53.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
weatherDF.count()
```




    2559




```python
# TEST - Run this cell to test your solution
cols = set(weatherDF.columns)

dfTest("ET2-P-04-01-01", 2559, weatherDF.count())
dfTest("ET2-P-04-01-02", True, "TAVG" in cols and "UNIT" in cols)

print("Tests passed!")
```

    Tests passed!


### Step 2: Define Complex Output Type

Define the complex output type for your UDF.  This should look like the following:

| Field Name | Type |
|:-----------|:-----|
| fahrenheit | Float |
| celsius | Float |
| kelvin | Float |


```python
# TODO
import pyspark.sql.types as T

schema = T.StructType([
            T.StructField('fahrenheit', T.FloatType(), True),
            T.StructField('celsius', T.FloatType(), True),
            T.StructField('kelvin', T.FloatType(), True)
        ])
```


```python
# TEST - Run this cell to test your solution
from pyspark.sql.types import FloatType
names = [i.name for i in schema.fields]

dfTest("ET2-P-04-02-01", 3, len(schema.fields))
dfTest("ET2-P-04-02-02", [FloatType(), FloatType(), FloatType()], [i.dataType for i in schema.fields])
dfTest("ET2-P-04-02-03", True, "fahrenheit" in names and "celsius" in names and "kelvin" in names)

print("Tests passed!")
```

    Tests passed!


### Step 3: Create the Function

Create a function that takes `temperature` as a Float and `unit` as a String.  `unit` will either be `F` for fahrenheit or `C` for celsius.  
Return a tuple of floats of that value as `(fahrenheit, celsius, kelvin)`.

Use the following equations:

| From | To Fahrenheit | To Celsius | To Kelvin |
|:-----|:--------------|:-----------|:-----------|
| Fahrenheit | F | (F - 32) * 5/9 | (F - 32) * 5/9 + 273.15 |
| Celsius | (C * 9/5) + 32 | C | C + 273.15 |
| Kelvin | (K - 273.15) * 9/5 + 32 | K - 273.15 | K |


```python
# TODO
def temperatureConverter(value, unit):
    C, F, K = 0, 0, 0
    if unit == "C" or unit == "c":
        C = value
        F = (value * (9/5)) + 32
        K = (value + 273.15)
    elif unit == "F" or unit == "f":
        C = (value - 32) * (5/9)
        F = value
        K = (value-32) * (5/9) + 273.15
    elif unit == "K" or unit == "k":
        C = value - 273.15
        F = (value - 273.15) * (9/5) + 32
        K = value
        
    return (F, C, K)
```


```python
temperatureConverter(90, "c")
```




    (194.0, 90, 363.15)




```python
# TEST - Run this cell to test your solution
dfTest("ET2-P-04-03-01", (194.0, 90, 363.15), temperatureConverter(90, "C"))
dfTest("ET2-P-04-03-02", (0, -17.77777777777778, 255.3722222222222), temperatureConverter(0, "F"))

print("Tests passed!")
```

    Tests passed!


### Step 4: Register the UDF

Register the UDF as `temperatureConverterUDF`


```python
# TODO
temperatureConverterUDF = spark.udf.register('temperatureConverterSQLUDF', temperatureConverter, schema)
```


```python
# TEST - Run this cell to test your solution
dfTest("ET2-P-04-04-01", (194.0, 90, 363.15), temperatureConverterUDF.func(90, "C"))
dfTest("ET2-P-04-04-02", (0, -17.77777777777778, 255.3722222222222), temperatureConverterUDF.func(0, "F"))

print("Tests passed!")
```

    Tests passed!


### Step 5: Apply your UDF

Create `weatherEnhancedDF` with a new column `TAVGAdjusted` that applies your UDF.


```python
# TODO
weatherEnhancedDF = weatherDF.withColumn('TAVGAdjusted', temperatureConverterUDF(F.col('TAVG'), F.col('UNIT')))
```


```python
display(weatherEnhancedDF)
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
      <th>NAME</th>
      <th>STATION</th>
      <th>LATITUDE</th>
      <th>LONGITUDE</th>
      <th>ELEVATION</th>
      <th>DATE</th>
      <th>UNIT</th>
      <th>TAVG</th>
      <th>TAVGAdjusted</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>HAYWARD AIR TERMINAL, CA US</td>
      <td>USW00093228</td>
      <td>37.654202</td>
      <td>-122.114998</td>
      <td>13.100000</td>
      <td>2018-05-27</td>
      <td>F</td>
      <td>61.0</td>
      <td>(61.0, 16.11111068725586, 289.2611083984375)</td>
    </tr>
    <tr>
      <th>1</th>
      <td>BIG ROCK CALIFORNIA, CA US</td>
      <td>USR0000CBIR</td>
      <td>38.039398</td>
      <td>-122.570000</td>
      <td>457.200012</td>
      <td>2018-01-05</td>
      <td>C</td>
      <td>11.7</td>
      <td>(53.060001373291016, 11.699999809265137, 284.8...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SAN FRANCISCO INTERNATIONAL AIRPORT, CA US</td>
      <td>USW00023234</td>
      <td>37.619701</td>
      <td>-122.364700</td>
      <td>2.400000</td>
      <td>2018-02-24</td>
      <td>C</td>
      <td>8.3</td>
      <td>(46.939998626708984, 8.300000190734863, 281.45...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>LAS TRAMPAS CALIFORNIA, CA US</td>
      <td>USR0000CTRA</td>
      <td>37.833900</td>
      <td>-122.066902</td>
      <td>536.400024</td>
      <td>2018-03-26</td>
      <td>C</td>
      <td>9.4</td>
      <td>(48.91999816894531, 9.399999618530273, 282.549...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>HOUSTON INTERCONTINENTAL AIRPORT, TX US</td>
      <td>USW00012960</td>
      <td>29.980000</td>
      <td>-95.360001</td>
      <td>29.000000</td>
      <td>2018-05-25</td>
      <td>F</td>
      <td>80.0</td>
      <td>(80.0, 26.66666603088379, 299.8166809082031)</td>
    </tr>
    <tr>
      <th>5</th>
      <td>BIG ROCK CALIFORNIA, CA US</td>
      <td>USR0000CBIR</td>
      <td>38.039398</td>
      <td>-122.570000</td>
      <td>457.200012</td>
      <td>2018-05-16</td>
      <td>C</td>
      <td>11.1</td>
      <td>(51.97999954223633, 11.100000381469727, 284.25)</td>
    </tr>
    <tr>
      <th>6</th>
      <td>BLACK DIAMOND CALIFORNIA, CA US</td>
      <td>USR0000CBKD</td>
      <td>37.950001</td>
      <td>-121.884399</td>
      <td>487.700012</td>
      <td>2018-05-25</td>
      <td>C</td>
      <td>10.6</td>
      <td>(51.08000183105469, 10.600000381469727, 283.75)</td>
    </tr>
    <tr>
      <th>7</th>
      <td>LAS TRAMPAS CALIFORNIA, CA US</td>
      <td>USR0000CTRA</td>
      <td>37.833900</td>
      <td>-122.066902</td>
      <td>536.400024</td>
      <td>2018-05-21</td>
      <td>C</td>
      <td>11.7</td>
      <td>(53.060001373291016, 11.699999809265137, 284.8...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>WOODACRE CALIFORNIA, CA US</td>
      <td>USR0000CWOO</td>
      <td>37.990601</td>
      <td>-122.644699</td>
      <td>426.700012</td>
      <td>2018-05-26</td>
      <td>F</td>
      <td>53.0</td>
      <td>(53.0, 11.666666984558105, 284.8166809082031)</td>
    </tr>
    <tr>
      <th>9</th>
      <td>BRIONES CALIFORNIA, CA US</td>
      <td>USR0000CBRI</td>
      <td>37.944199</td>
      <td>-122.117798</td>
      <td>442.000000</td>
      <td>2018-04-08</td>
      <td>F</td>
      <td>53.0</td>
      <td>(53.0, 11.666666984558105, 284.8166809082031)</td>
    </tr>
  </tbody>
</table>
</div>




```python
weatherEnhancedDF.select('TAVGAdjusted').first()
```




    Row(TAVGAdjusted=Row(fahrenheit=61.0, celsius=16.11111068725586, kelvin=289.2611083984375))




```python
# TEST - Run this cell to test your solution
result = weatherEnhancedDF.select("TAVGAdjusted").first()[0].asDict()

dfTest("ET2-P-04-05-01", {'fahrenheit': 61.0, 'celsius': 16.11111068725586, 'kelvin': 289.2611083984375}, result)
dfTest("ET2-P-04-05-02", 2559, weatherEnhancedDF.count())

print("Tests passed!")
```

    Tests passed!


## Review

**Question:** How do UDFs handle multiple column inputs and complex outputs?   
**Answer:** UDFs allow for multiple column inputs.  Complex outputs can be designated with the use of a defined schema encapsulate in a `StructType()` or a Scala case class.

**Question:** How can I do vectorized UDFs in Python and are they as performant as built-in functions?   
**Answer:** Spark 2.3 includes the use of vectorized UDFs using Pandas syntax. Even though they are vectorized, these UDFs will not be as performant built-in functions, though they will be more performant than non-vectorized Python UDFs.

&copy; 2019 [Intellinum Analytics, Inc](http://www.intellinum.co). All rights reserved.<br/>
