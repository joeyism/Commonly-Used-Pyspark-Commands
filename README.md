# Commonly Used Pyspark Commands

The following is a list of commonly used [Pyspark](http://spark.apache.org/docs/latest/api/python/index.html) commands that I have found to be useful.

DISCLAIMER: These are not the only ways to use these commands. There are obviously many other ways. These are just ways that I use often and have found to be useful.

## Table of Content
* [`Read and Write`](#read-and-write)
* [`Select`](#select)
* [`Apply Anonymous Function`](#apply-anonymous-function)
* [`Zip`](#zip)
* [`Count Distinct`](#count-distinct)
* [`Joins`](#joins)
* [`Groups and Aggregate`](#groups-and-aggregate)
* [`Working with datetime`](#working-with-datetime)
* [`Viewing Data`](#viewing-data)



### Read and Write
#### Read CSV to DataFrame
```python
df = sqlContext.read.load("data/file.csv",
    format="com.databricks.spark.csv",
    header="true", inferSchema="true",
    delimiter=',')
```

#### Write DataFrame to 1 CSV
```python
df.toPandas().to_csv("df.csv", index=False)

```

#### Write to csv but in multiple files in a folder
```python
df.write.csv("df_folder")

## or if you want to specify number of files

no_of_files = 256 # setting to 1 might throw error due to memory problems
df.coalesce(no_of_files).write.format("com.databricks.spark.csv").save("df_folder") 
```

### Select
#### Selecting one columns from dataframe
```python
df_new = df.select("col1")
```


#### Selecting multiple columns from dataframe
```python
df_new = df.select("col1", "col2")
df_new = df.select(["col1", "col2"])

```

### Apply Anonymous Function

#### Creating an anonymous function, and using that to transform column of data
```python
from pyspark.sql.functions import udf
df = df.where("new_col", udf(lambda x: x + 2)("col1"))


## More explicit example
from pyspark.sql.functions import udf
add_two_udf = udf(lambda x: x+2)
df = df.where("new_col", add_two_udf("col1")) # then df will have what it originally had, with new_col

```


#### Creating an anonymous function and making it return an integer
```python
from pyspark.sql.types import IntegerType
df = df.where("new_col", udf(lambda x: x + 2, IntegerType())("col1"))
df = df.where("new_col", udf(lambda x: x + 2)("col1").cast(IntegerType()))

```


#### Have a global variable, and reference that in anonymous function
```python
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

city_to_num = {"Toronto": 1, "Montreal": 2, "Vancouver": 3}
global_city_to_num = sc.broadcast(city_to_num)
city_to_num_udf = udf(lambda x: global_city_to_num.value[x], IntegerType())
df = df.where("city_id", city_to_num_udf("city_name"))

```

### Zip

#### Get unique values from a column, and zip it with a unique number so it returns a dict
```python
# So that the result is {"Toronto": 1, "Montreal": 2, "Vancouver": 3, ...}
city_to_num = dict(df.select("city_name").rdd.distinct().map(lambda x: x[0]).zipWithIndex().collect())

```

### Count Distinct

#### Number of distinct/unique values from a column
```python

#CPU times: user 0 ns, sys: 4 ms, total: 4 ms
#Wall time: 426 ms
df.select("col1").distinct().count()

# CPU times: user 16 ms, sys: 0 ns, total: 16 ms
# Wall time: 200 ms
df.select("col1").rdd.distinct().count()

```

### Joins

#### Joining two dataframes on different ids
```python
df_new = df1.join(df2, df1["some_id"] == df2["other_id"], "inner")
```

#### Joining two dataframes on the same id, and you don't want that id to repeat
```python
df_new = df1.join(df2, "id", "inner")
```

#### Join on multiple conditions
```python
df_new = df1.join(
            df2, 
            (df2["col1"] == df1["col2"]) & (df2["col3"] == df1["col4"]), 
            "inner"
        )
```

### Groups and Aggregate

#### Group by an id, and sum up values based on the groupby value

```python
from pyspark.sql.functions import sum

df_new = df.groupBy("store_id").agg(sum("no_of_customers").alias("total_no_of_customers")) 
```

#### Group by multiple, and aggregate multiple

```python
from pyspark.sql.functions import sum, avg

df_new = df.groupBy([
            "store_id",
            "date"
        ]).agg(
            sum("no_of_customers").alias("total_no_of_customers"),
            avg("no_of_customers").alias("avg_no_of_customers")
        )
```

#### Group by with custom sum aggregate

```python
from pyspark.sql.functions import udf
from pyspark.sql.functions import sum, avg

df_new = df.groupBy("store_id").agg(
            sum(udf(lambda t: t.weekday() == 4 or t.weekday() == 5, BooleanType())("visit_date").cast("int")).alias("no_of_visits_weekend")
        )


## A more explicit version
is_weekend = udf(lambda t: t.weekday() == 4 or t.weekday() == 5, BooleanType())
df_new = df.groupBy("store_id").agg(
            sum(
                is_weekend("visit_date").cast("int")
            ).alias("no_of_visits_weekend")
        )
```

### Working with datetime

#### Get day of the week
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

get_dotw = udf(lambda x: x.weekday(), IntegerType())
df = df.withColumn("dotw", get_dotw("date"))
```

#### Get 1 if it is a weekend, 0 if it is not
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

get_weekend = udf(lambda x: 1 if x.weekday() == 4 or x.weekday() == 5 else 0, IntegerType())
df = df.withColumn("dotw", get_weekend("date"))
```

#### Get day
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

get_day = udf(lambda x: x.day, IntegerType())
df = df.withColumn("dotw", get_day("date"))
```

#### Get Month
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

get_month = udf(lambda x: x.month, IntegerType())
df = df.withColumn("dotw", get_month("date"))
```

#### Convert String to Timestamp/datetime
If the string is of the form `yyyy-MM-dd`, this creates a new column that with the same data but in `timestamp` format. When you `.take` the value, it'll actually say it's `datetime.datetime` which is useful for manipulation

```python
from pyspark.sql.functions import col

df = df.select("*", col("time_string").cast("timestamp").alias("time_datetime")) 
```

### Viewing Data
#### Taking a peek at 1 row of data in list form
I use this alot, similar to when I use `df.head()` in pandas

```python
# CPU times: user 28 ms, sys: 4 ms, total: 32 ms                                  
# Wall time: 4.18 s
df.take(1)

# CPU times: user 8 ms, sys: 28 ms, total: 36 ms                                  
# Wall time: 4.19 s
df.head(1) # defaults to 1 if no specified
```

#### Looking at the entire set in list form
**WARNING**: This takes a while if your dataset is large. I don't usually use this

```python
df.collect()
```

### Machine Learning

#### Transforming for Training
I'm just using GBTRegressor as a example
```python
# Transforming
from pyspark.ml.linalg import DenseVector

df_train = df.select("output", "input1", "input2", ...)
df_train = df_train.rdd.map(lambda x: (x[0], DenseVector(x[1:])))
df_train = spark.createDataFrame(df_train, ["label", "features"])

# Traning
from pyspark.ml.regression import GBTRegressor
gbt = GBTRegressor(maxIter=10)
gbt.fit(df_train)


```
