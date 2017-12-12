# Commonly Used Pyspark Commands

The following is a list of commonly used [Pyspark](http://spark.apache.org/docs/latest/api/python/index.html) commands


#### CSV to DataFrame
```python
df = sqlContext.read.load("data/file.csv",
    format="com.databricks.spark.csv",
    header="true", inferSchema="true",
    delimiter=',')
```


#### Selecting one columns from dataframe
```python
new_df = df.select("col1")
```


#### Selecting multiple columns from dataframe
```python
new_df = df.select("col1", "col2")
new_df = df.select(["col1", "col2"])

```


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


#### Get unique values from a column, and zip it with a unique number so it returns a dict
```python
# So that the result is {"Toronto": 1, "Montreal": 2, "Vancouver": 3, ...}
city_to_num = dict(df.select("city_name").rdd.distinct().map(lambda x: x[0]).zipWithIndex().collect())

```


#### Number of distinct/unique values from a column
```python

#CPU times: user 0 ns, sys: 4 ms, total: 4 ms
#Wall time: 426 ms
df.select("col1").distinct().count()

# CPU times: user 16 ms, sys: 0 ns, total: 16 ms
# Wall time: 200 ms
df.select("col1").rdd.distinct().count()


```
