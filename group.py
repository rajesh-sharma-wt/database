import csv
# import json
from sort import external_merge_sort
import pandas as pd
import re
# import ipdb
# import math

def extract_aggregate_function_details(input_string):
    pattern = re.compile(r'(min|max|count|sum|avg)\(([\w\.]+)\)')
    matches = pattern.findall(input_string)
    details_list = [(match[0], match[1]) for match in matches]
    return details_list

# def extract_aggregate_functions(input_string):
#     pattern = re.compile(r'(\w+\([\w\*\s]+\))')
#     aggregate_functions = pattern.findall(input_string)
#     return aggregate_functions

def extract_function_and_argument(element):
    match = re.match(r'([a-zA-Z_]+)\(([^)]+)\)', element)
    if match:
        return match.group(1), match.group(2)
    return None, None

def extract_aggregate_function_from_cols(input_string):
    pattern = re.compile(r'(min|max|count|sum|avg)\(([\w\.]+)\)')
    match = pattern.search(input_string)

    if match:
        function_name = match.group(1)
        inner_content = match.group(2)
        return function_name, inner_content
    else:
        return None, None

def group_data(csv_file, group_by_col, columns, table=None):
    
    is_sorted, result = external_merge_sort(f'{csv_file}.csv', group_by_col, "ASC", table, chunk_size=500, output_filename='output_sorted.csv')
    if not is_sorted:
        return False, {
            'message': 'Error in sorting'
        }
    chunk_size = 100
    aggregate_columns = [(func, arg) for func, arg in (extract_function_and_argument(element) for element in columns) if func is not None and arg is not None]
    non_aggregate_columns = [element for element in columns if element not in (e[0] + '(' + e[1] + ')' for e in aggregate_columns)]

    if len(aggregate_columns) + 1 != len(columns):
        return False, {
            'message': 'Group by should have aggregate functions and only one column which is present in group by'
        }
    current_row = None
    count = max_val = min_val = sum = avg_sum = avg_count = 0
    filename = result['file_name'] + '.csv'
    grouped_file_name = csv_file + '_grouped'
    with open(f'{grouped_file_name}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(non_aggregate_columns + [e[0] + '(' + e[1] + ')' for e in aggregate_columns])
        for chunk in pd.read_csv(filename, chunksize=chunk_size, sep=','):
            df = chunk
            for row in df.iterrows():
                if current_row is None:
                    for (k, v) in aggregate_columns:
                        if v == '*' and k != 'count':
                            return False,{
                                'message': 'Invalid query'
                            }
                        if k == 'count':
                            if v == '*' or v != '*' and row[1][v] is not None:
                                count = 1
                        if k == 'max':
                            max_val = row[1][v]
                        elif k == 'min':
                            min_val = row[1][v]
                        elif k == 'sum':
                            sum = row[1][v]
                        elif k == 'avg':
                            avg_count = 1
                            avg_sum = row[1][v]
                            avg = avg_sum/avg_count
                    current_row = row[1][group_by_col]
                elif (current_row != row[1][group_by_col]) :
                    final_list = [current_row]
                    for (k, v) in aggregate_columns:
                        if v == '*' and k != 'count':
                            return False,{
                                'message': 'Invalid query'
                            }
                        if k == 'count':
                            final_list.append(count)
                            if v == '*' or (v != '*' and row[1][v] is not None):
                                count = 1 
                            elif v!= '*' and row[1][v] is None:
                                count = 0
                                continue
                        elif k == 'max':
                            final_list.append(max_val)
                            max_val = row[1][v]
                        elif k == 'min':
                            final_list.append(min_val)
                            min_val = row[1][v]
                        elif k == 'sum':
                            final_list.append(sum)
                            sum = row[1][v]
                        elif k == 'avg':
                            final_list.append(avg)
                            avg_count = 1
                            avg_sum = row[1][v]
                            avg = avg_sum
                    writer.writerow(final_list)
                    current_row = row[1][group_by_col]
                elif current_row == row[1][group_by_col]:
                    for (k, v) in aggregate_columns:
                        if v == '*' and k != 'count':
                            return False,{
                                'message': 'Invalid query'
                            }
                        if k == 'count':
                            if v == '*' or v != '*' and row[1][v] is not None:
                                count += 1
                            elif v!= '*' and row[1][v] is None:
                                continue
                        elif k == 'max':
                            max_val = max(row[1][v], max_val)
                        elif k == 'min':
                            min_val = min(row[1][v], min_val)
                        elif k == 'sum':
                            sum += row[1][v]
                        elif k == 'avg':
                            avg_count += 1
                            avg_sum += row[1][v]
                            avg = avg_sum/avg_count
    return True, {
        'message': 'Group by query executed successfully',
        'file_name': grouped_file_name
    }

def convert_to_backticks(expression):
    pattern = re.compile(r'(\w+)\(([^)]*)\)')
    result = pattern.sub(r'`\1(\2)`', expression)
    return result

def having_data(csv_file, having_clause, table=None):
    chunk_size = 100
    i=0
    total_rows = get_row_count(csv_file)
    filter_string = convert_to_backticks(having_clause)
    headers = pd.read_csv(f'{csv_file}.csv', nrows=1).columns
    for s_row in range(0, total_rows, chunk_size):
        df = pd.read_csv(f'{csv_file}.csv', skiprows=s_row, nrows=chunk_size, sep=',')
        df.columns = headers
        df = df[df.eval(filter_string)]
        if i == 0:
            df.to_csv(f'{table}_group_having.csv', index=False, header=True)
        else:
            df.to_csv(f'{table}_group_having.csv', index=False, header=False, mode='a')
        i += 1
    
    return True, {
        'message': 'Having query executed successfully',
        'file_name': f'{table}_group_having'
    }

def get_row_count(table):
    chunksize = 100
    count = 0
    for chunk in pd.read_csv(f'{table}.csv', chunksize=chunksize, encoding='utf-8', sep=','):
        df = chunk
        count += len(df)
    return count


        
def process_data(csv_file, group_by_col, columns, having_clause, order_by=None, table=None):
    if having_clause is not None:
        result = extract_aggregate_function_details(having_clause)
        if result is not None and len(result) > 0:
            columns = columns + [e[0] + '(' + e[1] + ')' for e in result if (e[0] + '(' + e[1] + ')') not in columns]
    if order_by is not None:
         result = extract_aggregate_function_details(order_by)
         if result is not None and len(result) > 0:
            columns = columns + [e[0] + '(' + e[1] + ')' for e in result if (e[0] + '(' + e[1] + ')') not in columns]
    if group_by_col in columns:
        is_grouped, result = group_data(csv_file, group_by_col, columns, table)
        
        if not is_grouped:
            return False, result
        if having_clause is None:
            return True, result["file_name"]
        if having_clause is not None:
            is_having, result = having_data(result["file_name"], having_clause, table)
            if not is_having:
                return False, result
            return True, result["file_name"]
    else:
        print("Invalid query please enter valid query")
        return False, {
            'message': 'Invalid query'
        }

def perform_group_by_operations(table, json_data):
    return process_data(table, json_data["group_by"] , json_data["columns"], json_data["having_clause"], json_data["order_by"], json_data["table"])
