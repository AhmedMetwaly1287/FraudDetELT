from kafka import KafkaConsumer
import pymysql
import json

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'fraud'

COL_NAMES = ['accountNumber', 'customerId', 'creditLimit', 'availableMoney', 'transactionDateTime', 'transactionAmount', 'merchantName', 'acqCountry', 'merchantCountryCode', 'posEntryMode', 'posConditionCode', 'merchantCategoryCode', 'currentExpDate', 'accountOpenDate', 'dateOfLastAddressChange', 'cardCVV', 'enteredCVV', 'cardLast4Digits', 'transactionType', 'echoBuffer', 'currentBalance', 'merchantCity', 'merchantState', 'merchantZip', 'cardPresent', 'posOnPremises', 'recurringAuthInd', 'expirationDateKeyInMatch', 'isFraud']

def establishConnection():
    host = "localhost"
    port = 3306
    database = "bigdataproj"
    username = "root"
    password = ""
    
    conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
    return conn

def insertIntoDatabase(connection, data):
    dbCursor = connection.cursor()

    try:
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

        # Prepare the SQL query with placeholders and execute
        sqlQuery = f'INSERT INTO transaction ({", ".join(COL_NAMES)}) VALUES ({", ".join(["%s"] * len(COL_NAMES))})'

        # Execute the query with the extracted values
        dbCursor.execute(sqlQuery, values)
        connection.commit()

    except Exception as e:
        print(f"Error occurred while processing data: {e}")

    finally:
        dbCursor.close()

try:
    dbConn = establishConnection()

    # Create KafkaConsumer
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='Kafka-Consumer1', value_deserializer=lambda msg: json.loads(msg.decode('utf-8')))

    consumer.subscribe(topics=KAFKA_TOPIC)

    for message in consumer:
        data = message.value
        insertIntoDatabase(dbConn, data)

    print("Data streamed to the database successfully")

except Exception as err:
    print(f"Error occurred: {err}")
