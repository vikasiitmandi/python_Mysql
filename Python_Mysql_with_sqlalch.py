import csv
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.exc import SQLAlchemyError

class DatabaseManager:
    def __init__(self, host, user, database):
        self.host = host
        self.user = user
        self.database = database
        self.engine = None
        self.metadata = MetaData()

    def create_engine(self):
        """Create a SQLAlchemy engine."""
        connection_string = f"mysql+mysqlconnector://{self.user}@{self.host}/{self.database}"
        self.engine = create_engine(connection_string)
        self.metadata.bind = self.engine

    def close_engine(self):
        """Close the SQLAlchemy engine."""
        if self.engine:
            self.engine.dispose()

    def insert_data(self, table_name, data):
        """Insert data into the MySQL database."""
        if not self.engine:
            raise ValueError("Engine not created. Call create_engine() first.")

        # Reflect the table from the database
        table = Table(table_name, self.metadata, autoload_with=self.engine, schema=None)

        # Skip the first row (header) and convert the rest to a list of dictionaries for insertion
        data_dicts = [dict(zip(data[0], row)) for row in data[1:]]

        with self.engine.connect() as connection:
            trans = connection.begin()
            try:
                # Insert data into the MySQL table
                connection.execute(table.insert().values(data_dicts))
                trans.commit()
                print('Data inserted successfully.')
            except SQLAlchemyError as e:
                trans.rollback()
                print(f'Error inserting data: {e}')
                import traceback
                traceback.print_exc()

# Replace 'your_file.csv', 'your_table' with the actual CSV file path and MySQL table name
table_name = 'Persons'

# Create a DatabaseManager instance with your MySQL credentials
db_manager = DatabaseManager(host='localhost', user='root', database='vikas')

# Read CSV file into a list of lists
with open('Ass.csv', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    data = list(csv_reader)

# Establish a connection to the MySQL server
db_manager.create_engine()

# Insert data into the MySQL database
db_manager.insert_data(table_name, data)

# Close the SQLAlchemy engine
db_manager.close_engine()
