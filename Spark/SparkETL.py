from pyspark.sql import SparkSession
from pyspark.sql.functions import col,substr,mode,when

ORDERED_COLS = ["id", "accountNumber", "creditLimit", "availableMoney", "currentBalance", "transactionDate","transactionTime", "transactionAmount", "merchantName", "acqCountry", "merchantCountryCode", "posEntryMode", "posConditionCode", "merchantCategoryCode", "cardPresent", "currentExpDate", "accountOpenDate", "dateOfLastAddressChange", "cardCVV", "enteredCVV", "cardLast4Digits", "transactionType", "expirationDateKeyInMatch", "isFraud"]

spark = SparkSession.builder \
    .appName("Data Transformation") \
    .getOrCreate()

#Reading Raw Data from the MySQL Database
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/frauddet") \
    .option("dbtable", "fraudtransactions") \
    .option("user", "root") \
    .option("password", "") \
    .load()

print("Raw data read from MySQL Database Successfully.")

#Columns with only NULL Values, Does not contribute to the EDA process due to Data Loss
COLUMNS_TO_DROP = ['echoBuffer','merchantState','merchantZip','recurringAuthInd','posOnPremises','merchantCity']

df = df.drop(*COLUMNS_TO_DROP)

#Dropping customerId as it is equivalent to accountNumber (Could cause redundant data)
df=df.drop('customerId')

#Creating new features to faciliate analysis
df = df.withColumn('transactionDate',col('transactionDateTime').substr(0,10))

df = df.withColumn('transactionTime',col('transactionDateTime').substr(11,19))

df = df.drop('transactionDateTime')

#Re-Ordering Columns
OrderedDF = df.select(*ORDERED_COLS)

#Filling NULL values in the 'acqCountry' using the Mode Value
acqCountryMode = OrderedDF.select(mode("merchantCountryCode")).first()[0]
FinalDF = OrderedDF.withColumn("merchantCountryCode", when(OrderedDF['merchantCountryCode']=='',f"{acqCountryMode}").otherwise(df['merchantCountryCode']))

print("Data Transformed Successfully.")

FinalDF.write.format("jdbc") \
    .option("url", f"jdbc:mysql://localhost:3306/frauddet") \
    .option("dbtable", f"processedtransactions") \
    .option("user", "root") \
    .option("password", "") \
    .mode("overwrite") \
    .save()

print("Transformed Data Loaded into MySQL Database Successfully.")

spark.stop()