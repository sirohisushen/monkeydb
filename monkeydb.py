import os
import json
import re
import uuid
import threading
from typing import Dict, List, Any, Union
from datetime import datetime

class StorageEngine:
    def __init__(self, base_path: str = 'database_storage'):
        """
        Initialize the storage engine with a base storage path

        Args:
            base_path (str): Base directory for storing database files
        """
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.lock = threading.Lock()

    def create_table_file(self, table_name: str) -> str:
        """
        Create a new table file

        Args:
            table_name (str): Name of the table

        Returns:
            str: Path to the created table file
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        with open(table_path, 'w') as f:
            json.dump([], f)
        return table_path

    def write_data(self, table_name: str, data: List[Dict[str, Any]]):
        """
        Write data to a table file

        Args:
            table_name (str): Name of the table
            data (List[Dict]): Data to write
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        with self.lock:
            with open(table_path, 'w') as f:
                json.dump(data, f, indent=2)

    def read_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Read data from a table file

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict]: Data from the table
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        if not os.path.exists(table_path):
            return []

        with self.lock:
            with open(table_path, 'r') as f:
                return json.load(f)

class QueryParser:
    @staticmethod
    def parse_create_table(query: str) -> Dict[str, Any]:
        """
        Parse CREATE TABLE query

        Args:
            query (str): CREATE TABLE query

        Returns:
            Dict: Parsed table information
        """
        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")

        table_name = match.group(1)
        columns_str = match.group(2)

        columns = []
        for column in columns_str.split(','):
            column = column.strip()
            name, data_type = column.split()
            columns.append({
                'name': name,
                'type': data_type
            })

        return {
            'type': 'create_table',
            'table_name': table_name,
            'columns': columns
        }

    @staticmethod
    def parse_insert(query: str) -> Dict[str, Any]:
        """
        Parse INSERT query

        Args:
            query (str): INSERT query

        Returns:
            Dict: Parsed insert information
        """
        pattern = r'INSERT\s+INTO\s+(\w+)\s*\((.*?)\)\s*VALUES\s*\((.*?)\)'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid INSERT syntax")

        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',')]
        values = [val.strip().strip("'\"") for val in match.group(3).split(',')]

        return {
            'type': 'insert',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }

    @staticmethod
    def parse_select(query: str) -> Dict[str, Any]:
        """
        Parse SELECT query

        Args:
            query (str): SELECT query

        Returns:
            Dict: Parsed select information
        """
        pattern = r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid SELECT syntax")

        columns = [col.strip() for col in match.group(1).split(',')]
        table_name = match.group(2)
        where_clause = match.group(3)

        return {
            'type': 'select',
            'columns': columns,
            'table_name': table_name,
            'where': where_clause
        }

class DatabaseEngine:
    def __init__(self, name: str = 'default_db'):
        """
        Initialize the database engine

        Args:
            name (str): Name of the database
        """
        self.name = name
        self.storage_engine = StorageEngine()
        self.tables = {}

    def execute_query(self, query: str) -> Union[List[Dict], None]:
        """
        Execute a SQL-like query

        Args:
            query (str): Query to execute

        Returns:
            Result of the query
        """
        query = query.strip()

        try:
            if query.upper().startswith('CREATE TABLE'):
                parsed_query = QueryParser.parse_create_table(query)
                return self._create_table(parsed_query)

            elif query.upper().startswith('INSERT INTO'):
                parsed_query = QueryParser.parse_insert(query)
                return self._insert(parsed_query)

            elif query.upper().startswith('SELECT'):
                parsed_query = QueryParser.parse_select(query)
                return self._select(parsed_query)

            else:
                raise ValueError(f"Unsupported query type: {query}")

        except Exception as e:
            print(f"Query execution error: {e}")
            return None

    def _create_table(self, parsed_query: Dict[str, Any]):
        """
        Create a new table

        Args:
            parsed_query (Dict): Parsed CREATE TABLE query
        """
        table_name = parsed_query['table_name']
        self.tables[table_name] = parsed_query['columns']
        self.storage_engine.create_table_file(table_name)
        return f"Table {table_name} created successfully"

    def _insert(self, parsed_query: Dict[str, Any]):
        """
        Insert data into a table

        Args:
            parsed_query (Dict): Parsed INSERT query
        """
        table_name = parsed_query['table_name']
        columns = parsed_query['columns']
        values = parsed_query['values']

        # Create record
        record = dict(zip(columns, values))
        record['id'] = str(uuid.uuid4())  # Add unique ID
        record['created_at'] = datetime.now().isoformat()

        # Read existing data
        existing_data = self.storage_engine.read_data(table_name)
        existing_data.append(record)

        # Write updated data
        self.storage_engine.write_data(table_name, existing_data)
        return "Record inserted successfully"

    def _select(self, parsed_query: Dict[str, Any]):
      """
      Select data from a table

      Args:
          parsed_query (Dict): Parsed SELECT query

      Returns:
          List of matching records
      """
      table_name = parsed_query['table_name']
      columns = parsed_query['columns']
      where_clause = parsed_query.get('where')
      data = self.storage_engine.read_data(table_name)

      print("Debug - Full Data:", data)  # Debug print
      print("Debug - Where Clause:", where_clause)  # Debug print

      if where_clause:
          # More robust filtering
          filtered_data = []
          for record in data:
              try:
                  # Split the where clause
                  field, value = [part.strip() for part in where_clause.split('=')]
                  value = value.strip("'\"")  # Remove quotes

                  print(f"Debug - Checking: {field} == {value}")  # Debug print
                  print(f"Debug - Record: {record}")  # Debug print

                  # Check if the record matches the condition
                  if str(record.get(field)) == str(value):
                      filtered_data.append(record)
              except Exception as e:
                  print(f"Error filtering record: {e}")

          # If columns is not '*', select only specified columns
          if columns[0] != '*':
              filtered_data = [{col: record.get(col) for col in columns} for record in filtered_data]

          print("Debug - Filtered Data:", filtered_data)  # Debug print
          return filtered_data
      else:
          # If columns is not '*', select only specified columns
          if columns[0] != '*':
              return [{col: record.get(col) for col in columns} for record in data]
          return data

# Modify the main execution to include more specific filtering
if __name__ == "__main__":
    db = DatabaseEngine()

    # Create a new table
    print(db.execute_query("CREATE TABLE users (id TEXT, name TEXT, age INT)"))

    # Insert records
    print(db.execute_query("INSERT INTO users (id, name, age) VALUES ('1', 'Alice', 30)"))
    print(db.execute_query("INSERT INTO users (id, name, age) VALUES ('2', 'Bob', 25)"))

    # Select records
    users = db.execute_query("SELECT * FROM users")
    print("Users:", users)

    # Select with a WHERE clause (using a specific ID)
    filtered_users = db.execute_query("SELECT * FROM users WHERE id = '1'")
    print("Filtered Users:", filtered_users)

    # Additional test with name filtering
    name_filtered_users = db.execute_query("SELECT * FROM users WHERE name = 'Alice'")
    print("Name Filtered Users:", name_filtered_users)