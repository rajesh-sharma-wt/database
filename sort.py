# create sorted chunks of the input file and store them in temporary files --> divide_into_sorted_chunks function does this
# merge_sorted_chunks function does the below things
# merge sorted chunks into a single output file.
# ExitStack is used to automatically open/manange/close all sorted temp files even if an exception occurs.
# mergepq.merge is a priority queue that gives the sorted order of the chunks. 
# it doesn't load all the files contents it has a generator that is it gets the next content once the first line is processed so there won't be any extra memory overhead
# once all the files are processed the sorted temp files are closed and deleted

import os
import heapq
import csv
import pandas as pd
from contextlib import ExitStack
import ipdb
import re

def external_merge_sort(input_filename, sort_by, sort_type, table=None, chunk_size=500, output_filename='output_sorted.csv'):
    try:
        sorted_chunk_filenames = divide_into_sorted_chunks(input_filename, sort_by, sort_type, chunk_size)
        merge_sorted_chunks(sorted_chunk_filenames, output_filename, sort_by, sort_type, table)
        return True, {
            'message': None,
            'file_name': 'output_sorted'
        }
    # write a catch block for the exception that is raised when the input file is empty
    except Exception as e:
        print(e)
        return False, {
            'message': 'Input file is empty'
        } 


def divide_into_sorted_chunks(input_filename, sort_by, sort_type, chunk_size):
    sorted_chunk_filenames = []
    chunk_number = 0
    for chunk in pd.read_csv(input_filename, chunksize=chunk_size):

        if sort_type.lower() == 'desc':
            chunk = chunk.sort_values(sort_by, ascending=False)
        else:
            chunk = chunk.sort_values(sort_by)

        sorted_chunk_filename = f'sorted_chunk_{chunk_number}.csv'
        chunk.to_csv(sorted_chunk_filename, index=False)
        sorted_chunk_filenames.append(sorted_chunk_filename)

        chunk_number += 1

    return sorted_chunk_filenames

def extract_function_and_argument(element):
    match = re.match(r'([a-zA-Z_]+)\(([^)]+)\)', element)
    if match:
        return match.group(1), match.group(2)
    return None, None

def merge_sorted_chunks(sorted_chunk_filenames, output_filename, sort_column, sort_type='asc', outer_table=None):
    def key_func(row):
        json_reader = pd.read_json('metadata.json')
        func, arg = extract_function_and_argument(sort_column)
        if func is not None and arg is not None:
            if arg == '*':
                return int(row[headers[0].index(sort_column)])
            new_sort_column = arg
        else:
            new_sort_column = sort_column
        
        if "." in new_sort_column:
            table = new_sort_column.split('.')[0]
            column_names = json_reader[table]["column_names"]
        else:
            table = outer_table
            column_names = json_reader[outer_table]["column_names"]
        
        for index, column in enumerate(column_names):
            column_names = json_reader[table]["column_names"]
            column_types = json_reader[table]["column_types"]
            if ('.' in sort_column and column == new_sort_column.split('.')[1]) or column == new_sort_column:
                if column_types[index] == 'int':
                    return int(row[headers[0].index(sort_column)])
                elif column_types[index] == 'float':
                    return float(row[headers[0].index(sort_column)])
                else:
                    return row[headers[0].index(sort_column)]
    
    with ExitStack() as stack:
        files = [stack.enter_context(open(file, 'r', newline='')) for file in sorted_chunk_filenames]
        with open(output_filename, 'w', newline='') as output_file:
            output_csv_writer = csv.writer(output_file)
            headers = [next(csv.reader(file)) for file in files]
            output_csv_writer.writerow(headers[0])
            file_iters = [csv.reader(file) for file in files]
            for line in heapq.merge(*file_iters, key=key_func, reverse=True if sort_type.lower() == 'desc' else False):
                output_csv_writer.writerow(line)

    # Clean up temporary files
    for file in sorted_chunk_filenames:
        os.remove(file)


# input_csv = 'student.csv'
# output_csv = 'output_sorted.csv'
# column_to_sort_by = 'id'
# table = 'student'

# external_merge_sort(input_csv, output_csv, column_to_sort_by, table)
