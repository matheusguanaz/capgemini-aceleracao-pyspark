from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType



if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Census Income]"))

	schema_census_income = StructType([
		StructField("age", IntegerType(), True),
		StructField("workclass", StringType(), True),
		StructField("fnlwgt", IntegerType(), True),
		StructField("education", StringType(), True),
		StructField("education-num", IntegerType(), True),
		StructField("marital-status", StringType(), True),
		StructField("occupation", StringType(), True),
		StructField("relationship", StringType(), True),
		StructField("race", StringType(), True),
		StructField("sex", StringType(), True),
		StructField("capital-gain", IntegerType(), True),
		StructField("capital-loss", IntegerType(), True),
		StructField("hours-per-week",IntegerType(), True),
		StructField("native-country",StringType(), True),
		StructField("income", StringType(), True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_census_income)
		          .load("./data/census-income/census-income.csv"))
	df.show()
