import pandas as pd
import os
import csv
import json
import ipdb


def execute_create_query(create_info):
    table = create_info["table_name"]
    columns = create_info["columns"]
    types = create_info["types"]
    pk = create_info["primary_key"] or None
    fk = create_info["foreign_key"] or None
    fk_col = create_info["references_column"] or None
    fk_table = create_info["references_table"] or None
    has_pk = create_info["has_primary_key"] or False
    has_fk = create_info["has_foreign_key"] or False

    if check_if_table_exists(table):
        return False, {
            'message': f'Table {table} already exists'
        }
    
    elif not validate_column_types(types):
        return False, {
            'message': f'Invalid column types'
        }
    
    if has_pk and pk not in columns:
        return False, {
            'message': f'Primary key {pk} not in columns'
        }
    
    if has_fk: 
        if fk not in columns:
            return False, {
                'message': f'Foreign key {fk} not in columns'
            }
        if not check_if_table_exists(fk_table):
            return False, {
                'message': f'Referenced table {fk_table} does not exist'
            }
        foreign_key_columns = get_all_columns(fk_table)
        if fk_col not in foreign_key_columns:
            return False, {
                'message': f'Referenced column {fk_col} does not exist'
            }


    #create a file with table.csv
    with open(f'{table}.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(columns)

    #update metadata.json
    try:
        with open("metadata.json", 'r') as json_file:
            existing_data = json.load(json_file)
    except FileNotFoundError:
        existing_data = {}
    
    new_data = {table: {"table": table, "column_names": columns, "column_types": types, "has_primary_key": pk is not None, "primary_key": pk if pk is not None else "", "foreign_key" : fk if fk is not None else "", "has_foreign_key": fk is not None, "foreign_key_table": fk_table if fk_table is not None else "", "foreign_key_column": fk_col if fk_col is not None else ""} }
    existing_data.update(new_data)
    with open("metadata.json", 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)
    
    return True, {
        'message': f'Table {table} created successfully'
    }


def check_if_table_exists(table):
    if not table:
        return False
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns.tolist():
        return True
    if os.path.exists(f'{table}.csv'):
        return True
    return False

def validate_column_types(types):
    if not types:
        return False
    valid_types = pd.read_json('metadata.json')['valid_types']['keys']
    for type in types:
        if type not in valid_types:
            return False
    return True

def get_all_columns(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns:
        return json_reader[table]["column_names"]
    return []

# if __name__ == '__main__':
#     if check_if_table_exists('test'):
#         print('Table already exists')
#     elif not validate_column_types(['int', 'str']):
#         print('Invalid column types')
#     else:
#         # this function should update the metadata.json file and then create the table with the given columns
#         execute_create_query('test', ['col 1', 'col 2'], ['int, str'], 'col 1')