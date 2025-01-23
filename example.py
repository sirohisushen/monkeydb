from monkeydb import DatabaseEngine
if __name__ == "__main__":
    # Initialize the database
    db = DatabaseEngine()

    # Test creating a table
    print(db.execute_query("CREATE TABLE users (id TEXT, name TEXT, age INT)"))

    # Test inserting data
    print(db.execute_query("INSERT INTO users (id, name, age) VALUES ('1', 'Shane', 16)"))
    print(db.execute_query("INSERT INTO users (id, name, age) VALUES ('2', 'Ceasor', 54)"))

    # Test selecting all users
    users = db.execute_query("SELECT * FROM users")
    print("Users:", users)

    # Test filtering by ID
    filtered_users = db.execute_query("SELECT * FROM users WHERE id = '1'")
    print("Filtered Users:", filtered_users)

    # Test filtering by Name
    name_filtered_users = db.execute_query("SELECT * FROM users WHERE name = 'Alice'")
    print("Name Filtered Users:", name_filtered_users)