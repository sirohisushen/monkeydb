from monkeydb import MonkeyDB

# Create an initial database
db = MonkeyDB()

# Create a table in database named 'db'
db.execute_query("CREATE TABLE users (id TEXT, name TEXT, age INT)")

# Insert a record to table 'users'
db.execute_query("INSERT INTO users (id, name, age) VALUES ('1', 'Alice', 30)")

# Select records from the table 'users'
users = db.execute_query("SELECT * FROM users")
print(users)