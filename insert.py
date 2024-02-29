import pandas as pd
import os
import ipdb
import csv

def check_if_table_exists(table):
    if os.path.exists(f'{table}.csv') and table in pd.read_json('metadata.json').columns.tolist():
        return True
    return False
    

# update this with the metadata.json file
def check_if_columns_exist(columns, table):
    total_columns = pd.read_json('metadata.json')[table].column_names
    for column in columns:
        if column not in total_columns:
            return column, False
    return None, True

def validate_primary_key(primary_key, table, primary_key_column):
    chunksize = 100
    for chunk in pd.read_csv(f'{table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
        if primary_key in chunk[primary_key_column].tolist():
            return False
    return True

def get_primary_key_column(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns:
        return json_reader[table]["has_primary_key"], json_reader[table]["primary_key"]
    return False, None

def get_all_columns(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns:
        return json_reader[table]["column_names"]
    return []
    
def validate_column_datatypes(columns, values, table):
    json_reader = pd.read_json('metadata.json')
    column_names = json_reader[table]["column_names"] 
    column_datatypes = json_reader[table]["column_types"]
    
    data = {}
    for index, column in enumerate(columns):
        data[column] = values[index]
    for index, column in enumerate(column_names):
        if column in data.keys():
            if not isinstance(data[column], eval(column_datatypes[index])):
                return False
    return True


def execute_insert_query(table, columns, values):
    file_path = f'{table}.csv'
    has_primary_key, primary_key_column = get_primary_key_column(table)
    if has_primary_key:
        if not validate_primary_key(values[0], table, primary_key_column):
            return False, {
                'message': f'Primary key violation'
            }
    all_columns = get_all_columns(table)
    
    new_data = {}
    for index, column in enumerate(columns):
        new_data[column] = values[index]

    new_row = []
    for column in all_columns:
        if column in new_data.keys():
            new_row.append(new_data[column])
        else:
            new_row.append(None)    
    csv.writer(open(file_path, 'a', newline=''), lineterminator='\n').writerow(new_row)   
    return True, {
        'message': f'Inserted successfully'
    }
            
    
   