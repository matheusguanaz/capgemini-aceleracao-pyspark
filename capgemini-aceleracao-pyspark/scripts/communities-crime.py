from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
 

def pergunta_1_qa(df):

	df = (df.withColumn('PolicOperBudg_qa',
	 			F.when(
					(F.col('PolicOperBudg').isNull()) | 
					(F.col('PolicOperBudg') == '?'),
				'M')
				.when(F.col('PolicOperBudg').contains(','), 'F')
				.when(F.col('PolicOperBudg').rlike('[^0-9.]'), 'A')))
				
	df.groupBy('PolicOperBudg_qa').count().show()


def pergunta_1_tr(df):

	df = df.withColumn('PolicOperBudg', F.col('PolicOperBudg').cast('double'))

	df.filter(F.col('PolicOperBudg').isNull()).groupBy('PolicOperBudg').count().orderBy(F.col('count').desc()).show()

	return df


if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Communities & Crime]"))

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          #.schema(schema_communities_crime)
				  .options(header=True, inferSChema=False)
		          .load("/home/spark/capgemini-aceleracao-pyspark/capgemini-aceleracao-pyspark/data/communities-crime/communities-crime.csv"))
	print(df.printSchema())

	pergunta_1_qa(df)
	df = pergunta_1_tr(df)