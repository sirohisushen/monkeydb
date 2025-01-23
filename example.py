from monkeydb import MonkeyDB

# Create a database
db = MonkeyDB()

# Create a table
db.execute_query("CREATE TABLE users (id TEXT, name TEXT, age INT)")

# Insert a record
db.execute_query("INSERT INTO users (id, name, age) VALUES ('1', 'Alice', 30)")

# Select records
users = db.execute_query("SELECT * FROM users")
print(users)