from pyspark.sql import SparkSession
import findspark

# findspark.init()

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Test APP") \
    .getOrCreate()

data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)
        ]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
## Creates a temporary view using the DataFrame
# schemaPeople.createOrReplaceTempView("people")
## SQL can be run over DataFrames that have been registered as a table.
results = spark.sql("SELECT name FROM people")

df.select(df['firstname'], df['salary'] + 1).show()
df.filter(df['salary'] > 3000).show()
df.groupBy("salary").count().show()

print(df.show())
