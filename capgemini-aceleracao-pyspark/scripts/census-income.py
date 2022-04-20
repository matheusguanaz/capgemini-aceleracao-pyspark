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
    
    education = ['Bachelors', 'Some-college', '11th', 'HS-grad', 'Prof-school',
	            'Assoc-acdm', 'Assoc-voc','9th', '7th-8th', '12th', 'Masters', 
                '1st-4th', '10th', 'Doctorate', '5th-6th', 'Preschool']
    
    df = df.withColumn('education_qa',
                                    F.when(F.col('education') == ' ?','M')
                                    .when(~F.col('education').isin(education),'C'))

    df.groupBy('education_qa').count().show()


def transformation_workclass(df):

    df = df.withColumn('workclass', F.regexp_replace('workclass',' ',''))
    df = df.withColumn('workclass',
                                F.when(F.col('workclass') == '?', None)
                                .otherwise(F.col('workclass')))

    df.groupBy('workclass').count().show()

    return df    
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
	
	quality_age(df)
	quality_workclass(df)
	quality_fnlwgt(df)

	df = transformation_workclass(df)


