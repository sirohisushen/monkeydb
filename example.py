from monkeydb import DatabaseEngine as _DBEngineAlias

def _initialize_database_instance() -> _DBEngineAlias:
    _db_instance = _DBEngineAlias()
    return _db_instance

def _execute_and_print(db_instance: _DBEngineAlias, query_string: str) -> None:
    _result = db_instance.execute_query(query_string)
    print(f"Executed Query: {query_string}\nResult: {_result}\n")

def _main():
    _database = _initialize_database_instance()

    _query_create_users_table = "CREATE TABLE users (id TEXT, name TEXT, age INT)"
    _execute_and_print(_database, _query_create_users_table)

    _insert_queries = [
        "INSERT INTO users (id, name, age) VALUES ('1', 'Shane', 16)",
        "INSERT INTO users (id, name, age) VALUES ('2', 'Ceasor', 54)"
    ]

    for _qry in _insert_queries:
        _execute_and_print(_database, _qry)

    _query_select_all = "SELECT * FROM users"
    _all_users = _database.execute_query(_query_select_all)
    print(f"Users: {_all_users}\n")

    _query_filter_by_id = "SELECT * FROM users WHERE id = '1'"
    _filtered_by_id = _database.execute_query(_query_filter_by_id)
    print(f"Filtered Users: {_filtered_by_id}\n")

    _query_filter_by_name = "SELECT * FROM users WHERE name = 'Alice'"
    _filtered_by_name = _database.execute_query(_query_filter_by_name)
    print(f"Name Filtered Users: {_filtered_by_name}\n")

if __name__ == "__main__":
    _main()
