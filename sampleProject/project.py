from flask import Flask, request, jsonify
from dbengine import DatabaseEngine

app = Flask(__name__)
db = DatabaseEngine()


db.execute_query("CREATE TABLE users (id TEXT, name TEXT, age INT)")


@app.route('/users', methods=['GET'])
def get_all_users():
    """Get all users"""
    users = db.execute_query("SELECT * FROM users")
    return jsonify(users), 200


@app.route('/users/<user_id>', methods=['GET'])
def get_user_by_id(user_id):
    """Get a specific user by ID"""
    user = db.execute_query(f"SELECT * FROM users WHERE id = '{user_id}'")
    if user:
        return jsonify(user), 200
    return jsonify({"error": "User not found"}), 404


@app.route('/users', methods=['POST'])
def create_user():
    """Create a new user"""
    data = request.json
    if not all(key in data for key in ('id', 'name', 'age')):
        return jsonify({"error": "Missing fields"}), 400

    query = f"INSERT INTO users (id, name, age) VALUES ('{data['id']}', '{data['name']}', {data['age']})"
    db.execute_query(query)
    return jsonify({"message": "User created successfully"}), 201


@app.route('/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    """Update an existing user"""
    data = request.json
    if not any(key in data for key in ('name', 'age')):
        return jsonify({"error": "No fields to update"}), 400

    users = db.execute_query("SELECT * FROM users")
    for user in users:
        if user['id'] == user_id:
            if 'name' in data:
                user['name'] = data['name']
            if 'age' in data:
                user['age'] = data['age']
            db.storage_engine.write_data("users", users)
            return jsonify({"message": "User updated successfully"}), 200

    return jsonify({"error": "User not found"}), 404


@app.route('/users/<user_id>', methods=['DELETE'])
def delete_user(user_id):
    """Delete a user by ID"""
    users = db.execute_query("SELECT * FROM users")
    filtered_users = [user for user in users if user['id'] != user_id]

    if len(filtered_users) == len(users):
        return jsonify({"error": "User not found"}), 404

    db.storage_engine.write_data("users", filtered_users)
    return jsonify({"message": "User deleted successfully"}), 200


if __name__ == "__main__":
    app.run(debug=True)
