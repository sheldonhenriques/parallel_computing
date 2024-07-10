import mysql.connector
import time
import logging

# Set up logging
logging.basicConfig(filename='mysql_write_test.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Connect to the database
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    password='', #add password here
    database="test"
)

try:
    cursor = mydb.cursor()

    # Create the table if it doesn't exist
    cursor.execute('CREATE TABLE IF NOT EXISTS foo (id INT AUTO_INCREMENT PRIMARY KEY, name TEXT)')

    # Measure the time for inserting data
    start_time = time.time()
    num_records = 2500
    logging.info(f'Inserting {num_records} records...')

    for i in range(num_records):
        name = 'user' + str(i)
        cursor.execute('INSERT INTO foo (name) VALUES (%s)', (name,))

    end_time = time.time()
    elapsed_time = end_time - start_time
    logging.info(f'Time taken to insert {num_records} records: {elapsed_time:.2f} seconds')

    # Read records
    cursor.execute('SELECT * FROM foo')
    result = cursor.fetchall()
    logging.info(f'Number of records fetched: {len(result)}')

    # Drop the table
    cursor.execute('DROP TABLE foo')

finally:
    # Close the cursor and connection
    cursor.close()
    mydb.close()