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
	print(df.filter(df.StockCode.startswith('gift_0001') & ~df.InvoiceNo.startswith('C'))
	.agg({'Quantity' : 'sum'})
	.show())


def pergunta_2_qa(df):

	df = df.withColumn("unitPrice_qa", 
						F.when(check_is_empty('UnitPrice'), 'M')
						.when(F.col('UnitPrice').contains(','), 'F')
						.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
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
	(df.filter(df.StockCode.startswith('gift_0001') & ~df.InvoiceNo.startswith('C'))
	.groupBy(F.month('InvoiceDate'))
	.sum('Quantity')
	.show())
	

def pergunta_3(df):
	print(df.filter(df.StockCode.startswith('S') & ~df.InvoiceNo.startswith('C'))
	.groupBy('StockCode').sum('Quantity')
	.show())


def pergunta_4(df):
	print(
		df
		.filter((~df.InvoiceNo.startswith('C')) & (df.Quantity > 0))
		.groupBy('StockCode')
		.sum('Quantity')
		.orderBy(F.col('sum(Quantity)').desc())
		.show()
	)

def pergunta_5_qa(df):

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))

	print(df.groupBy('InvoiceDate_qa').count().show()) 

	return df
	
def pergunta_5_tr(df):

	df = df.withColumn('InvoiceDate', 
							F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	print(df.filter(df.InvoiceDate.isNull()).show())

	return df

def pergunta_5(df):
	df = (
		df
		.filter((~df.InvoiceNo.startswith('C')) & (df.Quantity > 0))
		.groupBy('StockCode', F.month('InvoiceDate'))
		.sum('Quantity')
		.orderBy(F.col('sum(Quantity)').desc())
	)


	df = df.select('StockCode',
					F.col('month(InvoiceDate)').alias('month'),
					F.col('sum(Quantity)').alias('sum_quantity'))
	
	df_max_per_month = df.groupBy('month').max('sum_quantity')

	df_max_per_month = df_max_per_month.join(df.alias('b'), 
											F.col('b.sum_quantity') == F.col('max(sum_quantity)'),
											"left").select('b.month','StockCode','sum_quantity')
	
	print(df_max_per_month.orderBy('month').show())


def pergunta_6_qa(df):

	df = df.withColumn("UnitPrice_qa", 
						F.when(check_is_empty('UnitPrice'), 'M')
						.when(F.col('UnitPrice').contains(','), 'F')
						.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('StockCode_qa', 
						F.when(check_is_empty('StockCode'), 'M')
						.when(F.length(df.StockCode) != 5, 'F'))

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))


	print(df.groupBy('UnitPrice_qa').count().show())
	print(df.groupBy('StockCode_qa').count().show())	
	print(df.groupBy('InvoiceDate_qa').count().show()) 

	return df

def pergunta_6_tr(df):
	df = df.withColumn('InvoiceDate', 
							F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	print(df.filter(df.InvoiceDate.isNull()).show())

	df = df.withColumn('UnitPrice', 
							F.when(df['UnitPrice_qa'] == 'F', 
								F.regexp_replace('UnitPrice', ',','\\.'))
								.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	print(df.filter(df.UnitPrice.isNull()).show())
	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df

def pergunta_6(df):
	print(
		df
		.filter(df.valor_de_venda > 0)
		.groupBy(F.hour('InvoiceDate'))
		.sum('valor_de_venda')
		.orderBy(F.col('sum(valor_de_venda)').desc())
		.show()
	)

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

	#pergunta_3(df)

	#pergunta_4(df)

	#df = pergunta_5_qa(df)
	#df = pergunta_5_tr(df)
	#pergunta_5(df)

	df = pergunta_6_qa(df)
	df = pergunta_6_tr(df)
	pergunta_6(df)

