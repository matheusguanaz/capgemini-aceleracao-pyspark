from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def quality_age(df):
    df = df.withColumn('age_qa', 
	                            F.when(F.col('age').isNull(), 'M')
								.when(F.col('age') < 0 , 'N'))

    df.groupBy('age_qa').count().show()


def quality_workclass(df):
    workclasses = [' Private',' Self-emp-not-inc',' Self-emp-inc',' Federal-gov', 
                    ' Local-gov',' State-gov',' Without-pay',' Never-worked']

    df = df.withColumn('workclass_qa', 
                                    F.when(F.col('workclass') == ' ?','M')
                                    .when(~F.col('workclass').isin(workclasses),'C'))

    df.groupBy('workclass_qa').count().show()


def quality_fnlwgt(df):

    df = df.withColumn('fnlwgt_qa',
                                F.when(F.col('fnlwgt').isNull(),'M')
                                .when(F.col('fnlwgt') < 0, 'N'))
    
    df.groupBy('fnlwgt_qa').count().show()
        

def quality_education(df):
    
    education = [' Bachelors', ' Some-college', ' 11th', ' HS-grad', ' Prof-school',
	            ' Assoc-acdm', ' Assoc-voc',' 9th', ' 7th-8th', ' 12th', ' Masters', 
                ' 1st-4th', ' 10th', ' Doctorate', ' 5th-6th', ' Preschool']
    
    df = df.withColumn('education_qa',
                                    F.when(F.col('education') == ' ?','M')
                                    .when(~F.col('education').isin(education),'C'))

    df.groupBy('education_qa').count().show()


def quality_education_num(df):

    df = df.withColumn('education-num_qa',
                                F.when(F.col('education-num').isNull(),'M')
                                .when(F.col('education-num') < 0, 'N'))
    
    df.groupBy('education-num_qa').count().show()


def quality_marital_status(df):

    marital_status = [' Married-civ-spouse',' Divorced',' Never-married',' Separated',
                        ' Widowed',' Married-spouse-absent',' Married-AF-spouse']    
	
    df = df.withColumn('marital-status_qa',
                                        F.when(F.col('marital-status') == ' ?','M')
                                        .when(~F.col('marital-status').isin(marital_status),'C'))

    df.groupBy('marital-status_qa').count().show()   


def quality_occupation(df):
    
    occupations = [ ' Tech-support',' Craft-repair',' Other-service',' Sales',
	                ' Exec-managerial',' Prof-specialty',' Handlers-cleaners',
                    ' Machine-op-inspct',' Adm-clerical',' Farming-fishing',
                    ' Transport-moving',' Priv-house-serv',' Protective-serv',
                    ' Armed-Forces']    
	
    df = df.withColumn('occupation_qa',
                                        F.when(F.col('occupation') == ' ?','M')
                                        .when(~F.col('occupation').isin(occupations),'C'))

    df.groupBy('occupation_qa').count().show()


def quality_relationship(df):
    
    relationships = [' Wife',' Own-child',' Husband',' Not-in-family',' Other-relative',' Unmarried']

    df = df.withColumn('relationship_qa',
                                        F.when(F.col('relationship') == ' ?','M')
                                        .when(~F.col('relationship').isin(relationships),'C'))

    df.groupBy('relationship_qa').count().show()


def quality_race(df):

    races = [' White',' Asian-Pac-Islander',' Amer-Indian-Eskimo',' Other',' Black']

    df = df.withColumn('race_qa',
                                F.when(F.col('race') == ' ?','M')
                                .when(~F.col('race').isin(races),'C'))

    df.groupBy('race_qa').count().show()


def quality_sex(df):
    
    sex = [' Female',' Male']

    df = df.withColumn('sex_qa',
                                F.when(F.col('sex') == ' ?','M')
                                .when(~F.col('sex').isin(sex),'C'))

    df.groupBy('sex_qa').count().show()


def quality_capital_gain(df):

    df = df.withColumn('capital-gain_qa',
                                F.when(F.col('capital-gain').isNull(),'M')
                                .when(F.col('capital-gain') < 0, 'N'))
    
    df.groupBy('capital-gain_qa').count().show()


def quality_capital_loss(df):

    df = df.withColumn('capital-loss_qa',
                                F.when(F.col('capital-loss').isNull(),'M')
                                .when(F.col('capital-loss') < 0, 'N'))
    
    df.groupBy('capital-loss_qa').count().show()


def quality_hours_per_week(df):

    df = df.withColumn('hours-per-week_qa',
                                F.when(F.col('hours-per-week').isNull(),'M')
                                .when(F.col('hours-per-week') < 0, 'N'))
    
    df.groupBy('hours-per-week_qa').count().show()


def quality_native_country(df):

    countries = [' United-States',' Cambodia',' England',' Puerto-Rico',' Canada',
                ' Germany',' Outlying-US(Guam-USVI-etc)',' India',' Japan',' Greece',
                ' South',' China',' Cuba',' Iran',' Honduras',' Philippines',' Italy',
                ' Poland',' Jamaica',' Vietnam',' Mexico',' Portugal',' Ireland',' France',
                ' Dominican-Republic',' Laos',' Ecuador',' Taiwan',' Haiti',' Columbia',
                ' Hungary',' Guatemala',' Nicaragua',' Scotland',' Thailand',' Yugoslavia',
                ' El-Salvador',' Trinadad&Tobago',' Peru',' Hong',' Holand-Netherlands']

    df = df.withColumn('native-country_qa',
                                        F.when(F.col('native-country') == ' ?','M')
                                        .when(~F.col('native-country').isin(countries),'C'))

    df.groupBy('native-country_qa').count().show()


def quality_income(df):

    incomes = [' >50K',' <=50K']

    df = df.withColumn('income_qa',
                                F.when(F.col('income') == ' ?','M')
                                .when(~F.col('income').isin(incomes),'C'))

    df.groupBy('income_qa').count().show()


def transformation_workclass(df):

    df = df.withColumn('workclass', F.regexp_replace('workclass',' ',''))
    df = df.withColumn('workclass',
                                F.when(F.col('workclass') == '?', None)
                                .otherwise(F.col('workclass')))

    df.groupBy('workclass').count().show()

    return df


def transformation_education(df):

    df = df.withColumn('education', F.regexp_replace('education',' ',''))

    df.groupBy('education').count().show()

    return df


def transformation_marital_status(df):

    df = df.withColumn('marital-status', F.regexp_replace('marital-status',' ',''))

    df.groupBy('marital-status').count().show()

    return df


def transformation_occupation(df):

    df = df.withColumn('occupation', F.regexp_replace('occupation',' ',''))
    df = df.withColumn('occupation',
                                F.when(F.col('occupation') == '?', None)
                                .otherwise(F.col('occupation')))

    df.groupBy('occupation').count().show()

    return df


def transformation_relationship(df):

    df = df.withColumn('relationship', F.regexp_replace('relationship',' ',''))

    df.groupBy('relationship').count().show()

    return df


def transformation_race(df):

    df = df.withColumn('race', F.regexp_replace('race',' ',''))

    df.groupBy('race').count().show()

    return df


def transformation_sex(df):

    df = df.withColumn('sex', F.regexp_replace('sex',' ',''))

    df.groupBy('sex').count().show()

    return df


def transformation_native_country(df):

    df = df.withColumn('native-country', F.regexp_replace('native-country',' ',''))
    df = df.withColumn('native-country',
                                F.when(F.col('native-country') == '?', None)
                                .otherwise(F.col('native-country')))

    df.groupBy('native-country').count().show()

    return df


def transformation_income(df):

    df = df.withColumn('income', F.regexp_replace('income',' ',''))

    df.groupBy('income').count().show()

    return df


def pergunta_1(df):

    (df
	.select('workclass','income')
	.filter((F.col('income') == '>50K') & F.col('workclass').isNotNull())
	.groupBy('workclass')
	.count()
	.select('workclass')
	.show())


def pergunta_2(df):

	(df
	.groupBy('race')
	.avg('hours-per-week')
	.show())


def pergunta_3(df):

    numero_homens = df.filter(F.col('sex') == 'Male').agg({'sex':'count'}).first()[0]
    numero_mulheres = df.filter(F.col('sex') == 'Female').agg({'sex':'count'}).first()[0]
    numero_total = df.agg({'sex':'count'}).first()[0]
    
    print(f"Proporção homens {((numero_homens/numero_total)*100):.2f}%")
    print(f'Proporção mulheres {((numero_mulheres/numero_total)*100):.2f}%')


def pergunta_4(df):

    numero_homens = df.filter(F.col('sex') == 'Male').agg({'sex':'count'}).first()[0]
    numero_mulheres = df.filter(F.col('sex') == 'Female').agg({'sex':'count'}).first()[0]
    numero_total = df.agg({'sex':'count'}).first()[0]
    
    print(f"Proporção homens {((numero_homens/numero_total)*100):.2f}%")
    print(f'Proporção mulheres {((numero_mulheres/numero_total)*100):.2f}%')


def pergunta_5(df):

    maior_media_horas_trabalhadas_por_ocupacao = (df
                                                .groupBy('occupation')
                                                .avg('hours-per-week')
                                                .orderBy(F.col('avg(hours-per-week)').desc())
                                                .first()[1])

    (df
	.groupBy('occupation')
	.avg('hours-per-week')
	.filter(F.col('avg(hours-per-week)') == maior_media_horas_trabalhadas_por_ocupacao)
	.show())


def pergunta_6(df):

	df = (df
	.filter(F.col('occupation').isNotNull())
	.groupBy('occupation','education')
	.count())

	df_ocupacoes = df.groupBy('education').max()

	df = df.alias('b').join(df_ocupacoes.alias('a'),
	            (F.col('a.education') == F.col('b.education')) &
                (F.col('a.max(count)') == F.col('b.count')),
				"right").select('b.education','b.occupation','b.count')
	
	df.show()


def pergunta_7(df):
    
	df = (df
	.filter(F.col('occupation').isNotNull())
	.groupBy('occupation','sex')
	.count())

	df_ocupacoes_male = df.filter(F.col('sex') == 'Male')
	df_ocupacoes_female	= df.filter(F.col('sex') == 'Female')

	df_ocupacoes = df_ocupacoes_male.alias('male').join(df_ocupacoes_female.alias('female'),
                                                        F.col('male.occupation') == F.col('female.occupation'),
														"inner")
	df_ocupacoes = (df_ocupacoes
					.select(
						F.col('male.occupation').alias('ocupacao'),
						F.col('male.count').alias('num_homens'),
						F.col('female.count').alias('num_mulheres')
					))
	
	df_ocupacoes = df_ocupacoes.withColumn('soma', F.col('num_homens') + F.col('num_mulheres'))
	df_ocupacoes = df_ocupacoes.withColumn('diferenca', F.abs(F.col('num_homens') - F.col('num_mulheres')))

	df_ocupacoes.orderBy(F.col('soma').desc(),F.col('diferenca').asc()).show(1)
	

def pergunta_8(df):

	df_education_education_num = df.groupBy('education','education-num').count()

	df_race_education_num = df.groupBy('race').max('education-num')

	df_race_education = (df_race_education_num.alias('ren').join(df_education_education_num.alias('een'),
											F.col('ren.max(education-num)') == F.col('een.education-num'),
											"left").select('ren.race','een.education'))
	df_race_education.show()


def pergunta_9(df):
    
    (df
	.filter(F.col('workclass').startswith('Self-emp'))
	.groupBy('education','sex','race')
	.count()
	.orderBy(F.col('count').desc())
	.show(1))


def pergunta_10(df):    

    num_casados = df.filter(F.col('marital-status').startswith('Married')).agg(F.count('marital-status')).first()[0]
    num_nao_casados = df.filter(~F.col('marital-status').startswith('Married')).agg(F.count('marital-status')).first()[0]

    print(f'Para cada pessoa casada há {num_nao_casados/num_casados:.2f} não casadas')


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

    #quality_age(df)
    #quality_workclass(df)
    #quality_fnlwgt(df)
    #quality_education(df)
    #quality_education_num(df)
    #quality_marital_status(df)
    #quality_occupation(df)
    #quality_relationship(df)
    #quality_race(df)
    #quality_sex(df)
    #quality_capital_gain(df)
    #quality_capital_loss(df)
    #quality_hours_per_week(df)
    #quality_native_country(df)
    #quality_income(df)

    df = transformation_workclass(df)
    df = transformation_education(df)
    df = transformation_marital_status(df)
    df = transformation_occupation(df)
    df = transformation_relationship(df)
    df = transformation_race(df)
    df = transformation_sex(df)
    df = transformation_native_country(df)
    df = transformation_income(df)

    #pergunta_1(df)
    #pergunta_2(df)
    #pergunta_3(df)
    #pergunta_4(df)
    #pergunta_5(df)
    #pergunta_6(df)
    #pergunta_7(df)
    #pergunta_8(df)
    #pergunta_9(df)
    pergunta_10(df)