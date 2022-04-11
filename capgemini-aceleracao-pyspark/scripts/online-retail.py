from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_ISNOT_NUM = r'[^0-9]*'

def check_is_empty(col):
	return (F.col(col).isNull() | (F.col(col) == '') | F.col(col).rlike(REGEX_EMPTY_STR))

def pergunta_1_qa(df):

	df = df.withColumn("unitPrice_qa", 
						F.when(check_is_empty('unitPrice'), 'M')
						.when(F.col('unitPrice').contains(','), 'F')
						.when(F.col('unitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('StockCode_qa', 
						F.when(check_is_empty('StockCode'), 'M')
						.when(F.length(df.StockCode) != 5, 'F'))

	print(df.groupBy('unitPrice_qa').count().show())
	print(df.groupBy('StockCode_qa').count().show())

	return df


def pergunta_1_tr(df):

	df = df.withColumn('unitPrice', 
								F.when(df['unitPrice_qa'] == 'F', 
									F.regexp_replace('unitPrice', ',','\\.'))
									.otherwise(F.col('unitPrice'))
					)
	
	df = df.withColumn('unitPrice', F.col('unitPrice').cast('double'))

	df = df.withColumn('valor_de_venda', F.col('unitPrice') * F.col('Quantity'))
	
	print(df.filter(df.unitPrice.isNull()).show())

	return df


def pergunta_1(df):
	print(df.filter(df.StockCode.startswith('gift_0001')).agg({'Quantity' : 'sum'}).show())


def pergunta_2_qa(df):

	df = df.withColumn("unitPrice_qa", 
						F.when(check_is_empty('unitPrice'), 'M')
						.when(F.col('unitPrice').contains(','), 'F')
						.when(F.col('unitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('StockCode_qa', 
						F.when(check_is_empty('StockCode'), 'M')
						.when(F.length(df.StockCode) != 5, 'F'))

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))


	print(df.groupBy('unitPrice_qa').count().show())
	print(df.groupBy('StockCode_qa').count().show())	
	print(df.groupBy('InvoiceDate_qa').count().show()) 

	return df


def pergunta_2_tr(df):
	

	df = df.withColumn('InvoiceDate', 
							F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	print(df.filter(df.InvoiceDate.isNull()).show())

	df = df.withColumn('UnitPrice', 
							F.when(df['UnitPrice_qa'] == 'F', 
								F.regexp_replace('UnitPrice', ',','\\.'))
								.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df

def pergunta_2(df):
	(df.filter(df.StockCode.startswith('gift_0001'))
	.groupBy(F.month('InvoiceDate'))
	.sum('Quantity')
	.show())
	

def pergunta_3(df):
	print(df.filter(df.StockCode.startswith('S') & ~df.InvoiceNo.startswith('C'))
	.groupBy('StockCode').sum('Quantity')
	.show())	

if __name__ == "__main__":
	sc = SparkContext()
	spark = (SparkSession.builder.appName("Aceleração PySpark - Capgemini [Online Retail]"))

	schema_online_retail = StructType([
		StructField("InvoiceNo", StringType(), True),
		StructField("StockCode", StringType(), True),
		StructField("Description", StringType(), True),
		StructField("Quantity", IntegerType(), True),
		StructField("InvoiceDate", StringType(), True),
		StructField("UnitPrice", StringType(), True),
		StructField("CustomerID", StringType(), True),
		StructField("Country", StringType(), True)
	])

	df = (spark.getOrCreate().read
		          .format("csv")
		          .option("header", "true")
		          .schema(schema_online_retail)
		          .load("/home/spark/capgemini-aceleracao-pyspark/capgemini-aceleracao-pyspark/data/online-retail/online-retail.csv"))
	#print(df.show())

	#df = pergunta_1_qa(df)
	#df = pergunta_1_tr(df)
	#pergunta_1(df)

	#df = pergunta_2_qa(df)
	#df = pergunta_2_tr(df)
	#pergunta_2(df)

	pergunta_3(df)