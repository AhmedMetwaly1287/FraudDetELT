from kafka import KafkaConsumer
import pymysql
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'fraud'

COL_NAMES = ['accountNumber', 'customerId', 'creditLimit', 'availableMoney', 'transactionDateTime', 'transactionAmount', 'merchantName', 'acqCountry', 'merchantCountryCode', 'posEntryMode', 'posConditionCode', 'merchantCategoryCode', 'currentExpDate', 'accountOpenDate', 'dateOfLastAddressChange', 'cardCVV', 'enteredCVV', 'cardLast4Digits', 'transactionType', 'echoBuffer', 'currentBalance', 'merchantCity', 'merchantState', 'merchantZip', 'cardPresent', 'posOnPremises', 'recurringAuthInd', 'expirationDateKeyInMatch', 'isFraud']

def establishConnection():
    host = "localhost"
    port = 3306
    database = "frauddet"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    return conn

def insertIntoDatabase(connection, data):
    dbCursor = connection.cursor()
    values = (
        data.get('accountNumber', None),
        data.get('customerId', None),
        data.get('creditLimit', None),
        data.get('availableMoney', None),
        data.get('transactionDateTime', None),
        data.get('transactionAmount', None),
        data.get('merchantName', None),
        data.get('acqCountry', None),
        data.get('merchantCountryCode', None),
        data.get('posEntryMode', None),
        data.get('posConditionCode', None),
        data.get('merchantCategoryCode', None),
        data.get('currentExpDate', None),
        data.get('accountOpenDate', None),
        data.get('dateOfLastAddressChange', None),
        data.get('cardCVV', None),
        data.get('enteredCVV', None),
        data.get('cardLast4Digits', None),
        data.get('transactionType', None),
        data.get('echoBuffer', None),
        data.get('currentBalance', None),
        data.get('merchantCity', None),
        data.get('merchantState', None),
        data.get('merchantZip', None),
        data.get('cardPresent', None),
        data.get('posOnPremises', None),
        data.get('recurringAuthInd', None),
        data.get('expirationDateKeyInMatch', None),
        data.get('isFraud', None),
    )

    sqlQuery = f'INSERT INTO fraudtransactions ({", ".join(COL_NAMES)}) VALUES ({", ".join(["%s"] * len(COL_NAMES))})'

    # Batch insert data into the database
    dbCursor.execute(sqlQuery, values)
    connection.commit()

try:
    dbConn = establishConnection()

    # Create KafkaConsumer
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, auto_offset_reset = "earliest" ,value_deserializer=lambda msg: json.loads(msg.decode('utf-8')))

    consumer.subscribe(topics=KAFKA_TOPIC)
    try:
        for message in consumer:
                data = message.value
                insertIntoDatabase(dbConn, data)
    except Exception as err:
         print(f"Error Occured: {err}")
    else:
         print("Data streamed to database successfully.")
         dbConn.close()
         consumer.close()

except Exception as err:
    print(f"Error occurred: {err}")

