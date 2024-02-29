import pandas as pd
import sys
import ipdb
import os

def execute_join_operation(table, parent_col, join_table, child_col, columns=None):
    try:
        if os.path.exists(f'{table}_{join_table}.csv'):
            os.remove(f'{table}_{join_table}.csv')
        chunksize = 100
        i = 0 
        for chunk in pd.read_csv(f'{table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
            df = chunk
            for join_chunk in pd.read_csv(f'{join_table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
                join_df = join_chunk
                if i == 0:
                    col_headers = df.columns.tolist()
                    join_col_headers = join_df.columns.tolist()
                    new_cols_1 = {}
                    new_cols_2 = {}
                    for header in col_headers:
                        new_cols_1[header] = f'{table}.{header}'
                    for header in join_col_headers:
                        new_cols_2[header] = f'{join_table}.{header}'
                    df1 = df.rename(columns=new_cols_1, inplace=False)
                    join_df1 = join_df.rename(columns=new_cols_2, inplace=False)

                    cols = pd.DataFrame([list(df1.columns) + list(join_df1.columns)])

                    temp_csv_path = f'{table}_{join_table}.csv'
                    cols.to_csv(temp_csv_path, mode='a', header=False, index=False)
                    i += 1

                for _, row in df.iterrows():
                    for _, join_row in join_df.iterrows():
                        if row[parent_col] == join_row[child_col]:
                            merged_row = pd.concat([row, join_row])
                            merged_row = merged_row.to_frame().T
                            temp_csv_path = f'{table}_{join_table}.csv'
                            merged_row.to_csv(temp_csv_path, mode='a', header=False, index=False)
        return True, f'{table}_{join_table}'
    except Exception as e:
        print(e)
        return False, None

def print_merged_result(table, join_table, columns=None):
    chunksize = 100
    i = 0
    for chunk in pd.read_csv(f'{table}_{join_table}.csv' ,chunksize=chunksize, encoding='utf-8', sep=','):
        df = chunk
        if not df.empty:
                if columns is not None:
                    df.to_csv(sys.stdout, index=False, columns=columns, header=True, col_space=15)
                else:
                    df.to_csv(sys.stdout, index=False, header=True, col_space=15)
        print()
            

if __name__ == '__main__':
    execute_join_operation('student', 'take', 'id', 'sid')
    print_merged_result('student', 'take', 'id', 'sid', ['id', 'name', 'cno', 'semester'])