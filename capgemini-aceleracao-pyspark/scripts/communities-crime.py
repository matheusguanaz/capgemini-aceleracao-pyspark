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
	df = df.withColumn('crimes_violentos', F.col('ViolentCrimesPerPop') * F.col('population'))

	df.select('population').filter(F.col('population').isNull()).show()
	df.select('ViolentCrimesPerPop').filter(F.col('ViolentCrimesPerPop').isNull()).show()

	return df


def pergunta_2(df):

	print("Maior Numero de crimes violentos per capita")
	maior_n_crimes_violentos_per_capita = (df
	.agg({'ViolentCrimesPerPop':'max'})
	.first()[0])

	(df
	.select('communityname','ViolentCrimesPerPop')
	.filter(F.col('ViolentCrimesPerPop') == maior_n_crimes_violentos_per_capita)
	.show())

	print("Maior numero de crimes violentos")
	maior_n_crimes_violentos = (df
	.agg({'crimes_violentos':'max'})
	.first()[0])

	(df
	.select('communityname','crimes_violentos')
	.filter(F.col('crimes_violentos') == maior_n_crimes_violentos)
	.show())

def pergunta_3_qa(df):

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))

	df.groupBy('population_qa').count().show()


def pergunta_3_tr(df):

	df = df.withColumn('population', F.col('population').cast('double'))

	df.select('population').filter(F.col('population').isNull()).show()

	return df


def pergunta_3(df):

	maior_populacao = (df
				.agg({'population':'max'})
				.first()[0])

	(df
	.select('communityname','population')
	.filter(F.col('population') == maior_populacao)
	.show())

def pergunta_4_qa(df):

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))

	df = df.withColumn('racepctblack_qa',
				F.when(
					(F.col('racepctblack').isNull()) | 
					(F.col('racepctblack') == '?'),
				'M')
				.when(F.col('racepctblack').contains(','), 'F')
				.when(F.col('racepctblack').rlike('[^0-9.]'), 'A'))

	df.groupBy('population_qa').count().show()
	df.groupBy('racepctblack_qa').count().show()


def pergunta_4_tr(df):

	df = df.withColumn('population', F.col('population').cast('double'))
	df = df.withColumn('racepctblack', F.col('racepctblack').cast('double'))

	df = df.withColumn('populacao_negra', F.col('racepctblack') * F.col('population'))

	df.select('population').filter(F.col('population').isNull()).show()
	df.select('racepctblack').filter(F.col('racepctblack').isNull()).show()
	
	return df


def pergunta_4(df):

	print("Maior porcentagem de população negra")
	maior_porcentagem_populacao_negra = (df
										.agg({'racepctblack' : 'max'})
										.first()[0])
	
	(df
	.select('communityname','racepctblack')
	.filter(F.col('racepctblack') == maior_porcentagem_populacao_negra)
	.show())

	print("Maior população negra")
	maior_populacao_negra = (df
							.agg({'populacao_negra':'max'})
							.first()[0])

	(df
	.select('communityname','populacao_negra')
	.filter(F.col('populacao_negra') == maior_populacao_negra)
	.show())
	

def pergunta_5_qa(df):

	df = df.withColumn('PctEmploy_qa',
				F.when(
					(F.col('PctEmploy').isNull()) | 
					(F.col('PctEmploy') == '?'),
				'M')
				.when(F.col('PctEmploy').contains(','), 'F')
				.when(F.col('PctEmploy').rlike('[^0-9.]'), 'A'))

	df.groupBy('PctEmploy_qa').count().show()
	

def pergunta_5_tr(df):

	df = df.withColumn('PctEmploy', F.col('PctEmploy').cast('double'))

	df.select('PctEmploy').filter(F.col('PctEmploy').isNull()).show()

	return df


def pergunta_5(df):

	#Considerando que todos que recebem salário são empregados
	maior_percentual_recebe_salario = (df
									.agg({'PctEmploy':'max'})
									.first()[0])

	(df
	.select('communityname','PctEmploy')
	.filter(F.col('PctEmploy') == maior_percentual_recebe_salario)
	.show())


def pergunta_6_qa(df):

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))

	df = df.withColumn('agePct16t24_qa',
				F.when(
					(F.col('agePct16t24').isNull()) | 
					(F.col('agePct16t24') == '?'),
				'M')
				.when(F.col('agePct16t24').contains(','), 'F')
				.when(F.col('agePct16t24').rlike('[^0-9.]'), 'A'))

	df.groupBy('population_qa').count().show()
	df.groupBy('agePct16t24_qa').count().show()						


def pergunta_6_tr(df):

	df = df.withColumn('population', F.col('population').cast('double'))
	df = df.withColumn('agePct16t24', F.col('agePct16t24').cast('double'))

	df = df.withColumn('numero_jovens', F.col('agePct16t24') * F.col('population'))

	df.select('population').filter(F.col('population').isNull()).show()
	df.select('agePct16t24').filter(F.col('agePct16t24').isNull()).show()
	
	return df


def pergunta_6(df):
	#Considerando que jovem é definido como uma pessoa entre 16 e 24 anos
	print("Comunidade com maior percentual de jovens")
	maior_percentual_jovens = (df
							.agg({'agePct16t24' : 'max'})
							.first()[0])

	(df
	.select('communityname','agePct16t24')
	.filter(F.col('agePct16t24') == maior_percentual_jovens)
	.show())

	print("Comunidade com maior numero de jovens")
	maior_n_jovens = (df
						.agg({'numero_jovens' : 'max'})
						.first()[0])

	(df
	.select('communityname','numero_jovens')
	.filter(F.col('numero_jovens') == maior_n_jovens)
	.show())


def pergunta_7_qa(df):

	df = (df.withColumn('PolicOperBudg_qa',
	 			F.when(
					(F.col('PolicOperBudg').isNull()) | 
					(F.col('PolicOperBudg') == '?'),
				'M')
				.when(F.col('PolicOperBudg').contains(','), 'F')
				.when(F.col('PolicOperBudg').rlike('[^0-9.]'), 'A')))

	df = df.withColumn('ViolentCrimesPerPop_qa',
				F.when(
					(F.col('ViolentCrimesPerPop').isNull()) | 
					(F.col('ViolentCrimesPerPop') == '?'),
				'M')
				.when(F.col('ViolentCrimesPerPop').contains(','), 'F')
				.when(F.col('ViolentCrimesPerPop').rlike('[^0-9.]'), 'A'))
				
	df.groupBy('PolicOperBudg_qa').count().show()
	df.groupBy('ViolentCrimesPerPop_qa').count().show()


def pergunta_7_tr(df):

	df = df.withColumn('PolicOperBudg', F.col('PolicOperBudg').cast('double'))
	df = df.withColumn('ViolentCrimesPerPop', F.col('ViolentCrimesPerPop').cast('double'))


	df.filter(F.col('PolicOperBudg').isNull()).groupBy('PolicOperBudg').count().orderBy(F.col('count').desc()).show()
	df.filter(F.col('ViolentCrimesPerPop').isNull()).groupBy('ViolentCrimesPerPop').count().orderBy(F.col('count').desc()).show()

	return df


def pergunta_7(df):

	df = (df
		.select('PolicOperBudg','ViolentCrimesPerPop')
		.filter(F.col('PolicOperBudg').isNotNull()))

	print(df.stat.corr('PolicOperBudg','ViolentCrimesPerPop'))

def pergunta_8_qa(df):

	df = (df.withColumn('PolicOperBudg_qa',
	 			F.when(
					(F.col('PolicOperBudg').isNull()) | 
					(F.col('PolicOperBudg') == '?'),
				'M')
				.when(F.col('PolicOperBudg').contains(','), 'F')
				.when(F.col('PolicOperBudg').rlike('[^0-9.]'), 'A')))

	df = df.withColumn('PctPolicWhite_qa',
				F.when(
					(F.col('PctPolicWhite').isNull()) | 
					(F.col('PctPolicWhite') == '?'),
				'M')
				.when(F.col('PctPolicWhite').contains(','), 'F')
				.when(F.col('PctPolicWhite').rlike('[^0-9.]'), 'A'))
				
	df.groupBy('PolicOperBudg_qa').count().show()
	df.groupBy('PctPolicWhite_qa').count().show()


def pergunta_8_tr(df):

	df = df.withColumn('PolicOperBudg', F.col('PolicOperBudg').cast('double'))
	df = df.withColumn('PctPolicWhite', F.col('PctPolicWhite').cast('double'))


	df.filter(F.col('PolicOperBudg').isNull()).groupBy('PolicOperBudg').count().orderBy(F.col('count').desc()).show()
	df.filter(F.col('PctPolicWhite').isNull()).groupBy('PctPolicWhite').count().orderBy(F.col('count').desc()).show()

	return df


def pergunta_8(df):

	df = (df
		.select('PctPolicWhite','PolicOperBudg')
		.filter(
			F.col('PolicOperBudg').isNotNull() & 
			F.col('PctPolicWhite').isNotNull()
			)
		)
		
	print(df.stat.corr('PctPolicWhite','PolicOperBudg'))


def pergunta_9_qa(df):

	df = (df.withColumn('PolicOperBudg_qa',
	 			F.when(
					(F.col('PolicOperBudg').isNull()) | 
					(F.col('PolicOperBudg') == '?'),
				'M')
				.when(F.col('PolicOperBudg').contains(','), 'F')
				.when(F.col('PolicOperBudg').rlike('[^0-9.]'), 'A')))

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))
				
	df.groupBy('PolicOperBudg_qa').count().show()
	df.groupBy('population_qa').count().show()


def pergunta_9_tr(df):

	df = df.withColumn('PolicOperBudg', F.col('PolicOperBudg').cast('double'))
	df = df.withColumn('population', F.col('population').cast('double'))


	df.filter(F.col('PolicOperBudg').isNull()).groupBy('PolicOperBudg').count().orderBy(F.col('count').desc()).show()
	df.filter(F.col('population').isNull()).groupBy('population').count().orderBy(F.col('count').desc()).show()

	return df


def pergunta_9(df):

	df = (df
		.select('population','PolicOperBudg')
		.filter(F.col('PolicOperBudg').isNotNull())
		)
		
	print(df.stat.corr('population','PolicOperBudg'))


def pergunta_10_qa(df):

	df = (df.withColumn('ViolentCrimesPerPop_qa',
	 			F.when(
					(F.col('ViolentCrimesPerPop').isNull()) | 
					(F.col('ViolentCrimesPerPop') == '?'),
				'M')
				.when(F.col('ViolentCrimesPerPop').contains(','), 'F')
				.when(F.col('ViolentCrimesPerPop').rlike('[^0-9.]'), 'A')))

	df = df.withColumn('population_qa',
				F.when(
					(F.col('population').isNull()) | 
					(F.col('population') == '?'),
				'M')
				.when(F.col('population').contains(','), 'F')
				.when(F.col('population').rlike('[^0-9.]'), 'A'))
				
	df.groupBy('ViolentCrimesPerPop_qa').count().show()
	df.groupBy('population_qa').count().show()


def pergunta_10_tr(df):

	df = df.withColumn('ViolentCrimesPerPop', F.col('ViolentCrimesPerPop').cast('double'))
	df = df.withColumn('population', F.col('population').cast('double'))


	df.filter(F.col('ViolentCrimesPerPop').isNull()).groupBy('ViolentCrimesPerPop').count().orderBy(F.col('count').desc()).show()
	df.filter(F.col('population').isNull()).groupBy('population').count().orderBy(F.col('count').desc()).show()

	return df


def pergunta_10(df):

	df = (df.select('population','ViolentCrimesPerPop'))
		
	print(df.stat.corr('population','ViolentCrimesPerPop'))


def pergunta_11_qa(df):

	df = (df.withColumn('ViolentCrimesPerPop_qa',
	 			F.when(
					(F.col('ViolentCrimesPerPop').isNull()) | 
					(F.col('ViolentCrimesPerPop') == '?'),
				'M')
				.when(F.col('ViolentCrimesPerPop').contains(','), 'F')
				.when(F.col('ViolentCrimesPerPop').rlike('[^0-9.]'), 'A')))

	df = df.withColumn('medFamInc_qa',
				F.when(
					(F.col('medFamInc').isNull()) | 
					(F.col('medFamInc') == '?'),
				'M')
				.when(F.col('medFamInc').contains(','), 'F')
				.when(F.col('medFamInc').rlike('[^0-9.]'), 'A'))
				
	df.groupBy('ViolentCrimesPerPop_qa').count().show()
	df.groupBy('medFamInc_qa').count().show()


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

	#pergunta_2_qa(df)
	#df = pergunta_2_tr(df)
	#pergunta_2(df)

	#pergunta_3_qa(df)
	#df = pergunta_3_tr(df)
	#pergunta_3(df)

	#pergunta_4_qa(df)
	#df = pergunta_4_tr(df)
	#pergunta_4(df)

	#pergunta_5_qa(df)
	#df = pergunta_5_tr(df)
	#pergunta_5(df)

	#pergunta_6_qa(df)
	#df = pergunta_6_tr(df)
	#pergunta_6(df)

	#pergunta_7_qa(df)
	#df = pergunta_7_tr(df)
	#pergunta_7(df)

	#pergunta_8_qa(df)
	#df = pergunta_8_tr(df)
	#pergunta_8(df)

	#pergunta_9_qa(df)
	#df = pergunta_9_tr(df)
	#pergunta_9(df)

	#pergunta_10_qa(df)
	#df = pergunta_10_tr(df)
	#pergunta_10(df)

	pergunta_11_qa(df)