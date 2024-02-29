import os
import pandas as pd
import ipdb

def check_if_table_exists(table):
    json_reader = pd.read_json('metadata.json')
    if table in json_reader.columns.tolist():
        return True
    if os.path.exists(f'{table}.csv'):
        return True
    return False

def check_if_update_columns_exist(table, columns):
    json_reader = pd.read_json('metadata.json')
    total_columns = json_reader[table]["column_names"]
    for column in columns:
        if column not in total_columns:
            return column, False
    return None, True

def execute_update_query(table, columns, values, conditions=None):
    if not check_if_table_exists(table):
        return False, {
            'message': f'Table {table} does not exist'
        }
    
    column_name, does_columns_exist = check_if_update_columns_exist(table, columns)
    if not does_columns_exist:
        return False, {
            'message': f'Column {column_name} does not exist'
        }
    if len(columns) != len(values):
        return False, {
            'message': f'Number of columns and values do not match'
        }
    if conditions is None:
        return False, {
            'message': f'Conditions not provided'
        }
    chunksize = 100
    i = 0
    updated_record_count = 0
    for chunk in pd.read_csv(f'{table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
        df = chunk
        if not df.empty:
            if conditions:
                final_where_condition = f'({conditions})'
                val = df.eval(final_where_condition)
                updated_record_count += val.sum()
                for index, column in enumerate(columns):
                    df.loc[val, column] = values[index]
                if i == 0:
                    df.to_csv(f'{table}_new.csv', index=False, header=True)
                    i += 1
                else:
                    df.to_csv(f'{table}_new.csv', index=False, header=False, mode='a')
            else:
                updated_record_count += len(df)
                for index, column in enumerate(columns):
                    df[column] = values[index]
                if i == 0:
                    df.to_csv(f'{table}_new.csv', index=False, header=True)
                    i += 1
                else:
                    df.to_csv(f'{table}_new.csv', index=False, header=False, mode='a')
    os.remove(f'{table}.csv')
    os.rename(f'{table}_new.csv', f'{table}.csv')
    return True, {
        'message': f'Updated {updated_record_count} records successfully'
    }
                

if __name__ == '__main__':
    if not check_if_table_exists('test'):
        print('Table does not exist')
    col_name, does_columns_exist = check_if_update_columns_exist('test', ['col 1', 'col 2'])
    if not does_columns_exist:
        print(f'Column {col_name} does not exist')
    else:
        execute_update_query('')