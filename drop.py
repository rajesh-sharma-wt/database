import pandas as pd
import os
import json

def delete_key_from_json_file(file_path, key_to_delete):
    with open(file_path, 'r') as file:
        data = json.load(file)

    if key_to_delete in data:
        del data[key_to_delete]
        print(f"The key '{key_to_delete}' has been deleted.")
    else:
        print(f"The key '{key_to_delete}' does not exist.")

    with open(file_path, 'w') as file:
        json.dump(data, file, indent=2)

def check_if_table_exists(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns.tolist() and os.path.isfile(f'{table}.csv'):
        return True
    return False

def execute_drop_query(table):
    if not check_if_table_exists(table):
        print('Table does not exist')
        return False, None
    if os.path.exists(f'{table}.csv'):
        os.remove(f'{table}.csv')
    delete_key_from_json_file('metadata.json', table)
    return True, {
        'message': f'Table {table} dropped successfully'
    }
        