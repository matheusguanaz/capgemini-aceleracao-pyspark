from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

REGEX_EMPTY_STR = r'[\t ]+$'
REGEX_ISNOT_NUM = r'[^0-9]*'

def check_is_empty(col):
	return (F.col(col).isNull() | (F.col(col) == '') | F.col(col).rlike(REGEX_EMPTY_STR))

def pergunta_1_qa(df):

	df = df.withColumn('Quantity_qa', 
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('Quantity_qa').count().show()

	return df


def pergunta_1_tr(df):

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df.filter(F.col('Quantity').isNull()).show()

	return df


def pergunta_1(df):

	(df
	.filter((F.col('StockCode').startswith('gift_0001')) & #Apenas gift cards
			(~F.col('InvoiceNo').startswith('C')) & # Desconsidera vendas canceladas 
			(F.col('Quantity') > 0)) #Apenas vendas que valores que entraram em caixa
	.agg({'Quantity' : 'sum'})
	.show())


def pergunta_2_qa(df):

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))

	df = df.withColumn('Quantity_qa', 
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('Quantity_qa').count().show()
	df.groupBy('InvoiceDate_qa').count().show() 

	return df


def pergunta_2_tr(df):
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	df.filter(F.col('Quantity').isNull()).show()
	df.filter(F.col('InvoiceDate').isNull()).show()

	return df


def pergunta_2(df):
	
	(df
	.filter((F.col('StockCode').startswith('gift_0001')) & #Apenas gift cards
			(~F.col('InvoiceNo').startswith('C')) & # Desconsidera vendas canceladas
			(F.col('Quantity') > 0)) #Apenas vendas que valores que entraram em caixa
	.groupBy(F.month('InvoiceDate'))
	.sum('Quantity')
	.orderBy('month(InvoiceDate)')
	.show())
	

def pergunta_3_qa(df):

	df = df.withColumn('Quantity_qa', 
					F.when(F.col('Quantity').isNull(), 'M'))
	
	df.filter(F.col('Quantity').isNull()).show()
	df.groupBy('Quantity_qa').count().show()

	return df


def pergunta_3_tr(df):

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))
	
	df.filter(F.col('Quantity').isNull()).show()

	return df


def pergunta_3(df):

	(df
	.filter((F.col('StockCode') == 'S') & #Apenas amostras
			(~F.col('InvoiceNo').startswith('C')) & # Desconsidera vendas canceladas
			(F.col('Quantity') > 0)) #Apenas vendas que valores que entraram em caixa
	.groupBy('StockCode').sum('Quantity')
	.show())


def pergunta_4_qa(df):

	df.filter(F.col('Quantity').isNull()).show()
	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	return df

def pergunta_4_tr(df):

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df.filter(F.col('Quantity').isNull()).show()

	return df


def pergunta_4(df):

	(df
	.filter((~F.col('InvoiceNo').startswith('C')) & # Desconsidera vendas canceladas
			(F.col('Quantity') > 0) & #Apenas vendas que valores que entraram em caixa
			(F.col('StockCode') != 'PADS')) #Desconsiderar tipo PADS
	.groupBy('StockCode')
	.sum('Quantity')
	.orderBy(F.col('sum(Quantity)').desc())
	.show())


def pergunta_5_qa(df):

	df = df.withColumn('InvoiceDate_qa', 
						F.when(check_is_empty('InvoiceDate'), 'M'))

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('InvoiceDate_qa').count().show() 
	df.groupBy('Quantity_qa').count().show() 

	return df
	
	
def pergunta_5_tr(df):

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df.filter(F.col('InvoiceDate').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()

	return df


def pergunta_5(df):

	df = (
		df
		.filter((~F.col('InvoiceNo').startswith('C')) & # Desconsidera vendas canceladas
				(F.col('Quantity') > 0) & #Apenas vendas que valores que entraram em caixa
				(F.col('StockCode') != 'PADS')) #Desconsiderar tipo PADS
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
	
	df_max_per_month.orderBy('month').show()


def pergunta_6_qa(df):

	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))


	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	
	df.groupBy('InvoiceDate_qa').count().show() 

	return df


def pergunta_6_tr(df):

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	df.filter(F.col('InvoiceDate').isNull()).show()

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df.filter(F.col('Quantity.').isNull()).show()

	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_6(df):
	
	(df
	.filter(F.col('valor_de_venda') > 0 & #Apenas vendas que valores que entraram em caixa
	 		(F.col('StockCode') != 'PADS')) #Desconsiderar tipo PADS
	.groupBy(F.hour('InvoiceDate'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show())


def pergunta_7_qa(df):

	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))


	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	
	df.groupBy('InvoiceDate_qa').count().show() 

	return df


def pergunta_7_tr(df):

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	df.filter(F.col('InvoiceDate').isNull()).show()

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()
	
	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_7(df):
	
	(df
	.filter(F.col('valor_de_venda') > 0 &  #Apenas vendas que valores que entraram em caixa
			(F.col('StockCode') != 'PADS')) #Desconsiderar tipo PADS
	.groupBy(F.month('InvoiceDate'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show())


def pergunta_8_qa(df):
	
	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df = df.withColumn('InvoiceDate_qa', F.when(check_is_empty('InvoiceDate'), 'M'))

	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	
	df.groupBy('InvoiceDate_qa').count().show() 
	
	return df


def pergunta_8_tr(df):

	df = df.withColumn('InvoiceDate', 
					F.to_timestamp(F.col('InvoiceDate'), 'd/M/yyyy H:m'))

	df.filter(F.col('InvoiceDate').isNull()).show()

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()
	
	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_8(df):

	(df
	.filter((F.col('valor_de_venda') > 0) & #Considerar apenas entradas no caixa
			(F.col('StockCode') != 'PADS')) #Desconsiderar PADS
	.groupBy(F.month('InvoiceDate'), F.col('StockCode'))
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))


def pergunta_9_qa(df):

	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	

	return df
	 

def pergunta_9_tr(df):

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()

	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_9(df):

	(df
	.filter((F.col('valor_de_venda') > 0) & #Considerar apenas entradas no caixa
			(F.col('StockCode') != 'PADS')) #Desconsiderar produto PADS
	.groupBy('Country')
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))


def pergunta_10_qa(df):

	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	

	return df


def pergunta_10_tr(df):

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()

	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_10(df):
		
	(df
	.filter((F.col('valor_de_venda') > 0) & #Considerar apenas entradas no caixa
			(F.col('StockCode') != 'PADS') & #Desconsiderar tipo de produto PADS
			(F.col('StockCode') == 'M')) #Considerando apenas vendas manuais
	.groupBy('Country')
	.sum('valor_de_venda')
	.orderBy(F.col('sum(valor_de_venda)').desc())
	.show(1))


def pergunta_11_qa(df):

	df = df.withColumn("UnitPrice_qa", 
					F.when(check_is_empty('UnitPrice'), 'M')
					.when(F.col('UnitPrice').contains(','), 'F')
					.when(F.col('UnitPrice').rlike('[^0-9]'), 'A')
	)

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('UnitPrice_qa').count().show()
	df.groupBy('Quantity_qa').count().show()	

	return df	


def pergunta_11_tr(df):

	df = df.withColumn('UnitPrice', 
				F.when(df['UnitPrice_qa'] == 'F', 
					F.regexp_replace('UnitPrice', ',','\\.'))
				.otherwise(F.col('UnitPrice'))
				)
	
	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df = df.withColumn('UnitPrice', F.col('UnitPrice').cast('double'))

	df.filter(F.col('UnitPrice').isNull()).show()
	df.filter(F.col('Quantity').isNull()).show()

	df = df.withColumn('valor_de_venda', F.col('UnitPrice') * F.col('Quantity'))

	return df


def pergunta_11(df):

	maior_valor_de_venda = (df
				.filter(F.col('StockCode') != 'PADS') #desconsiderar tipo de produto PADS
				.agg({'valor_de_venda' : 'max'})
				.first()[0])

	(df
	.select('InvoiceNo','valor_de_venda')
	.filter(F.col('valor_de_venda') == maior_valor_de_venda)
	.show())


def pergunta_12_qa(df):

	df = df.withColumn('Quantity_qa',
					F.when(F.col('Quantity').isNull(), 'M'))

	df.groupBy('Quantity_qa').count().show()	

	return df


def pergunta_12_tr(df):

	df = df.withColumn('Quantity',
				F.when(F.col('Quantity_qa') == 'M', 0)
				.otherwise(F.col('Quantity')))

	df.filter(F.col('Quantity').isNull()).show()

	return df


def pergunta_12(df):

	maior_n_de_itens = (df
				.filter(F.col('StockCode') != 'PADS') #Desconsiderar tipos de produto PADS
				.agg({'Quantity' : 'max'})
				.first()[0])

	(df
	.select('InvoiceNo','Quantity')
	.filter(F.col('Quantity') == maior_n_de_itens)
	.show())


def pergunta_13_qa(df):

	df = df.withColumn('CustomerID_qa',
				F.when(F.col('CustomerID').isNull(), 'M'))
	
	df.groupBy('CustomerID_qa').count().show()


def pergunta_13(df):

	mais_frequente = (df
					.filter(
						(F.col('StockCode') != 'PADS') & # Desconsiderar tipo PADS
						(F.col('CustomerID').isNotNull()) #Desconsiderar clientes não informados
						)
					.groupBy('CustomerID')
					.count()
					.orderBy(F.col('count').desc())
					.first()[1])

	(df
	.filter(
		(F.col('StockCode') != 'PADS') & #Desconsiderar tipo PADS
		(F.col('CustomerID').isNotNull()) #Desconsiderar clientes não informados
		)
	.groupBy('CustomerID')
	.count()
	.filter(F.col('count') == mais_frequente)
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
		          .load("./data/online-retail/online-retail.csv"))
	#print(df.show())

	#df = pergunta_1_qa(df)
	#df = pergunta_1_tr(df)
	#pergunta_1(df)

	#df = pergunta_2_qa(df)
	#df = pergunta_2_tr(df)
	#pergunta_2(df)

	#df = pergunta_3_qa(df)
	#df = pergunta_3_tr(df)
	#pergunta_3(df)

	#df = pergunta_4_qa(df)
	#df = pergunta_4_tr(df)
	#pergunta_4(df)

	#df = pergunta_5_qa(df)
	#df = pergunta_5_tr(df)
	#pergunta_5(df)

	#df = pergunta_6_qa(df)
	#df = pergunta_6_tr(df)
	#pergunta_6(df)

	#df = pergunta_7_qa(df)
	#df = pergunta_7_tr(df)
	#pergunta_7(df)

	#df = pergunta_8_qa(df)
	#df = pergunta_8_tr(df)
	#pergunta_8(df)

	#df = pergunta_9_qa(df)
	#df = pergunta_9_tr(df)
	#pergunta_9(df)

	#df = pergunta_10_qa(df)
	#df = pergunta_10_tr(df)
	#pergunta_10(df)

	#df = pergunta_11_qa(df)
	#df = pergunta_11_tr(df)
	#pergunta_11(df)

	#df = pergunta_12_qa(df)
	#df = pergunta_12_tr(df)
	#pergunta_12(df)

	pergunta_13_qa(df)
	pergunta_13(df)