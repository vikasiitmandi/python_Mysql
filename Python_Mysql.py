import pandas as pd
import mysql.connector

# Function to establish a connection to the MySQL server
def create_connection():
    # Replace 'your_username', 'your_password', 'your_database', and 'your_host' with your actual MySQL credentials
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        database='vikas'
    )
    return connection

# Function to insert data into the MySQL database
def insert_data(connection, table_name, data):
    
    cursor = connection.cursor()
    columns = ', '.join(data.columns)
    placeholders = ', '.join(['%s'] * len(data.columns))
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    # Convert DataFrame to list of tuples for insertion
    data_tuples = [tuple(row) for row in data.values]

    # Execute the query with the provided values
    cursor.executemany(query, data_tuples)

    # Commit the changes to the database
    connection.commit()

    # Close the cursor
    cursor.close()

# Replace 'your_file.csv', 'your_table' with the actual CSV file path and MySQL table name
table_name = 'Persons'

# Read CSV file into a pandas DataFrame
data = pd.read_csv('Ass.csv')
print(data)

# Establish a connection to the MySQL server
connection = create_connection()

# Insert data into the MySQL database
insert_data(connection, table_name, data)
print('Done')

# Close the database connection
connection.close()
