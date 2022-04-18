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


def pergunta_1(df):

	maior_orcamento = (df
					.agg({'PolicOperBudg' : 'max'})
					.first()[0])

	df.select('communityname','PolicOperBudg').filter(F.col('PolicOperBudg') == maior_orcamento).show()


def pergunta_2_qa(df):

	df = df.withColumn('ViolentCrimesPerPop_qa',
				F.when(
					(F.col('ViolentCrimesPerPop').isNull()) | 
					(F.col('ViolentCrimesPerPop') == '?'),
				'M')
				.when(F.col('ViolentCrimesPerPop').contains(','), 'F')
				.when(F.col('ViolentCrimesPerPop').rlike('[^0-9.]'), 'A'))

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))

	df.groupBy('ViolentCrimesPerPop_qa').count().show()
	df.groupBy('population_qa').count().show()


def pergunta_2_tr(df):

	df = df.withColumn('population', F.col('population').cast('double'))
	df = df.withColumn('ViolentCrimesPerPop', F.col('ViolentCrimesPerPop').cast('double'))

	df.select('population').filter(F.col('population').isNull()).show()
	df.select('ViolentCrimesPerPop').filter(F.col('ViolentCrimesPerPop').isNull()).show()

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
	#df.printSchema()

	#pergunta_1_qa(df)
	#df = pergunta_1_tr(df)
	#pergunta_1(df)

	pergunta_2_qa(df)
	df = pergunta_2_tr(df)