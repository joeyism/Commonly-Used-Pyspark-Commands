# Commonly Used Pyspark Commands

The following is a list of commonly used [Pyspark](http://spark.apache.org/docs/latest/api/python/index.html) commands

## Table of Content
* [`Read and Write`](#read-and-write)
* [`Select`](#select)
* [`Apply Anonymous Function`](#apply-anonymous-function)
* [`Zip`](#zip)
* [`Count Distinct`](#count-distinct)
* [`Joins`](#joins)
* [`Groups and Aggregate`](#groups-and-aggregate)

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

