import pandas as pd
import os
import ipdb

def check_if_table_exists(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns.tolist() and os.path.exists(f'{table}.csv'):
        return True
    return False

def check_if_delete_columns_exist(table, columns):
    json_reader = pd.read_json('metadata.json')
    total_columns = json_reader[table]["column_names"]
    for column in columns:
        if column not in total_columns:
            return column, False
    return None, True

def execute_delete_query(table, conditions=None):
    if not check_if_table_exists(table):
        return False, {
            'message': f'Table {table} does not exist'
        }
    chunksize = 100
    i = 0
    for chunk in pd.read_csv(f'{table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
        df = chunk
        if conditions:
            final_where_condition = f'~({conditions})'
            df = df[df.eval(final_where_condition)]
            if i == 0:
                df.to_csv(f'{table}_new.csv', index=False, header=True)
                i += 1
            else:
                df.to_csv(f'{table}_new.csv', index=False, header=False, mode='a')
        else:
            df = pd.DataFrame([df.columns.tolist()])
            df.to_csv(f'{table}_new.csv', index=False, header=False)

    os.remove(f'{table}.csv')
    os.rename(f'{table}_new.csv', f'{table}.csv')
    return True, {
        'message': f'Deleted successfully'
    }
