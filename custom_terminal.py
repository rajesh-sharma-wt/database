import os
import ipdb
from query_parser import parse_sql
from select_file import execute_select_query
from drop import execute_drop_query
from delete import execute_delete_query
from update import execute_update_query
from create import execute_create_query
from insert import execute_insert_query

def custom_terminal():
    while True:
        # Get user input
        user_input = input("MyDB> (type 'exit' to quit) ")

        # Check if the user wants to exit
        if user_input.lower() == 'exit':
            print("Exiting the terminal.")
            break

        # Process and execute the command
        try:
            if user_input.strip() == '':
                continue
            isValid, parsed_query = parse_sql(user_input)
            if isValid:
                if parsed_query["type"].upper() == 'DROP':
                    print(execute_drop_query(parsed_query["table_name"]))
                elif parsed_query["type"].upper() == 'SELECT':
                    is_successful, result = execute_select_query(parsed_query["table"], parsed_query["columns"], parsed_query["where_clause"], parsed_query)
                    if not is_successful:
                        print(result["message"])
                elif parsed_query["type"].upper() == 'UPDATE':
                    print(execute_update_query(parsed_query["table_name"], parsed_query["columns"], parsed_query["values"], parsed_query["filters"]))
                elif parsed_query["type"].upper() == 'INSERT':
                    print(execute_insert_query(parsed_query["table_name"], parsed_query["columns"], parsed_query["values"]))
                elif parsed_query["type"].upper() == 'CREATE':
                    print(execute_create_query(parsed_query))
                elif parsed_query["type"].upper() == 'DELETE':
                    print(execute_delete_query(parsed_query["table_name"], parsed_query["filters"]))
            else:
                print(f"Invalid query: {user_input}")
            
        except Exception as e:
            print(f"Error executing command: {e}")

custom_terminal()