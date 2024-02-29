import pprint
import json
import pandas as pd
import ipdb
from sample_select_parse import parse_select_query
#from select_file import execute_select_query

#handle join clause

def convert_to_join(join_condition, table_name, column_name):
    if(join_condition==""):
        return ""
    if '.' not in join_condition[0] or '.' not in join_condition[2]:
        return ""
    first_parts = join_condition[0].split('.')
    second_parts = join_condition[2].split('.')

    first_table = first_parts[0]
    first_column = first_parts[1]

    second_table = second_parts[0]
    second_column = second_parts[1]

    join_conditions = [
        {'table_name': first_table, 'column_name': first_column},
        {'table_name': second_table, 'column_name': second_column}
    ]

    return join_conditions

def validate_column_types(types):
    valid_types = ['int', 'str', 'float']
    for type in types:
        if type not in valid_types:
            return False
    return True



#helper for handling where clause
def handle_where_clause(query):
    if(query==""):
        return ""
    or_conditions = query.split(' or ')
    table_conditions = {}

    for or_condition in or_conditions:
        and_conditions = or_condition.split(' and ')
        for condition in and_conditions:
            parts = condition.split('==')
            table_name, field = parts[0].strip().split('.')
            condition = parts[1].strip()

            if table_name in table_conditions:
                table_conditions[table_name].append(f"{field.strip()} == {condition.strip()}")
            else:
                table_conditions[table_name] = [f"{field.strip()} == {condition.strip()}"]

    final_conditions = []

    for table, conditions in table_conditions.items():
        final_conditions.append({'table_name': table, 'condition': ' and '.join(conditions)})

    return final_conditions


# Helper function to tokenize SQL queries
def tokenize(query):
    tokens = query.split()
    return tokens

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def process_list(items):
    processed_list = []
    for item in items:
        item = item.strip()
        if isinstance(item, str) and (item.startswith('"') and item.endswith('"')) or (item.startswith("'") and item.endswith("'")):
            processed_list.append(item[1:-1])
        else:
            try:
                if item.isdigit():
                    processed_list.append(int(item))
                elif is_float(item):
                    processed_list.append(float(item))  
            except ValueError:
                processed_list.append(item)
    return processed_list

# SQL Parser
def parse_sql(query):
    tokens = tokenize(query)
    if not tokens:
        return None
    if tokens[0].upper() == 'SELECT':
        return parse_select_query(query)
    elif tokens[0].upper() == 'CREATE':
        if ("create" not in tokens and "CREATE" not in tokens) or ("table" not in tokens and "TABLE" not in tokens):
            return False, {
                "error": "Invalid query",
                "message": "Missing Keywords"
            }
        if "primary key" in query or "PRIMARY KEY" in query:
            query = query.replace("primary key", "PRIMARY_KEY")
            query = query.replace("PRIMARY KEY", "PRIMARY_KEY")
        if "foreign key" in query or "FOREIGN KEY" in query:
            query = query.replace("foreign key", "FOREIGN_KEY")
            query = query.replace("FOREIGN KEY", "FOREIGN_KEY")
        
        tokens = tokenize(query)

        create_idx = tokens.index('CREATE') if 'CREATE' in tokens else tokens.index('create') if 'create' in tokens else None
        table_idx = tokens.index('TABLE') if 'TABLE' in tokens else tokens.index('table') if 'table' in tokens else None
        table_name = tokens[table_idx + 1]
        foreign_key = None
        references_table = None
        references_column = None
        primary_key = None
        has_primary_key = False
        has_foreign_key = False

        if 'PRIMARY_KEY' in tokens or 'primary_key' in tokens:
            has_primary_key = True
            primary_key_idx = tokens.index('PRIMARY_KEY') if 'PRIMARY_KEY' in tokens else tokens.index('primary_key') if 'primary_key' in tokens else None
            primary_key = tokens[primary_key_idx + 1]
            columns = tokens[table_idx + 2: primary_key_idx]
        if 'FOREIGN_KEY' in tokens or 'foreign_key' in tokens:
            has_foreign_key = True
            foreign_key_idx = tokens.index('FOREIGN_KEY') if 'FOREIGN_KEY' in tokens else tokens.index('foreign_key') if 'foreign_key' in tokens else None
            foreign_key = tokens[foreign_key_idx + 1]
            if 'PRIMARY_KEY' not in tokens or 'primary_key' not in tokens:
                columns = tokens[table_idx + 2: foreign_key_idx]
            if 'REFERENCES' not in tokens or 'references' not in tokens:
                return False, {
                    "error": "Invalid query",
                    "message": "Missing REFERENCES clause"
                }
            references_idx = tokens.index('REFERENCES') if 'REFERENCES' in tokens else tokens.index('references') if 'references' in tokens else None
            references_table = tokens[references_idx + 1]
            references_column = tokens[references_idx + 2]

        if ('PRIMARY_KEY' not in tokens and 'primary_key' not in tokens) and ('FOREIGN_KEY' not in tokens and 'foreign_key' not in tokens):
            columns = tokens[table_idx + 2:]
        
        if len(columns) % 2 != 0:
            return False, {
                "error": "Invalid query",
                "message": "Missing column type or column"
            }
        final_columns = list(map(lambda x: columns[x], range(0, len(columns), 2)))
        types = list(map(lambda x: columns[x], range(1, len(columns), 2)))

        validate_types = validate_column_types(types)

        if not validate_types:
            return False, {
                "error": "Invalid query",
                "message": "Invalid column types or column name has spaces in it. Please enter column names without spaces"
            }
            
        return True, {
            'type': 'CREATE',
            'table_name': table_name,
            'columns': final_columns,
            'types': types,
            'primary_key': primary_key,
            'foreign_key': foreign_key,
            'references_table': references_table,
            'references_column': references_column,
            'has_primary_key': has_primary_key,
            'has_foreign_key': has_foreign_key
        }
        
    elif tokens[0].upper() == 'INSERT':
        if ("insert" not in tokens and "INSERT" not in tokens) or ("into" not in tokens and "INTO" not in tokens) or ("values" not in tokens and "VALUES" not in tokens) or ("columns" not in tokens and "COLUMNS" not in tokens):
            return False, {
                "error": "Invalid query",
                "message": "Missing Keywords"
            }
        into_idx = tokens.index('INTO') if 'INTO' in tokens else tokens.index('into') if 'into' in tokens else None
        table_name = tokens[into_idx + 1]
        columns_idx = tokens.index('COLUMNS') if 'COLUMNS' in tokens else tokens.index('columns') if 'columns' in tokens else None
        values_idx = tokens.index('VALUES') if 'VALUES' in tokens else tokens.index('values') if 'values' in tokens else None
        if columns_idx is None or values_idx is None:
            return False, {
                "error": "Invalid query",
                "message": "Missing COLUMNS clause or VALUES clause"
            }
        columns = []
        values = []
        if "COLUMNS" in tokens:
            columns = query[query.index('COLUMNS')+len('COLUMNS'):query.index('VALUES')].strip().split(',')
        elif "columns" in tokens:
            columns = query[query.index('columns')+len('columns'):query.index('values')].strip().split(',')

        if "VALUES" in tokens:
            values = query[query.index('VALUES')+len('VALUES'):].strip().split(',')
        elif "values" in tokens:
            values = query[query.index('values')+len('values'):].strip().split(',')


        if columns is None or values is None:
            return False, {
                "error": "Invalid query",
                "message": "Missing column names or values"
            }

        columns = [item.strip() for item in columns if item.strip()]
        values = process_list(values)

        for col in columns:
            if " " in col:
                return False, {
                    "error": "Invalid query",
                    "message": "Column name has spaces in it. Please enter column names without spaces"
                } 

        if len(columns) != len(values):
            return False, {
                "error": "Invalid query",
                "message": "Number of columns and values do not match"
            }

        return True, {
            'type': 'INSERT',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }
    
    elif tokens[0].upper() == 'UPDATE':
        if ("update" not in tokens and "UPDATE" not in tokens) or ("set" not in tokens and "SET" not in tokens) or ("values" not in tokens and "VALUES" not in tokens):
            return False, {
                "error": "Invalid query",
                "message": "Missing Keywords"
            }
        update_idx = tokens.index('UPDATE') if 'UPDATE' in tokens else tokens.index('update') if 'update' in tokens else None
        table_name = tokens[update_idx + 1]
        if table_name.lower() == "set":
            return False, {
                "error": "Invalid query",
                "message": "Missing table name"
            }
        set_idx = tokens.index('SET') if 'SET' in tokens else tokens.index('set') if 'set' in tokens else None
        values_idx = tokens.index('VALUES') if 'VALUES' in tokens else tokens.index('values') if 'values' in tokens else None

        if not table_name or not set_idx:
            return False, {
                "error": "Invalid query",
                "message": "Missing SET clause or table name"
            }

        
        filters = None
        v_idx = None
        if 'VALUES' in tokens:
            v_idx = query.index('VALUES')
        elif 'values' in tokens:
            v_idx = query.index('values')
        if 'SET' in tokens:
            columns = query[query.index('SET')+len('SET'):v_idx].strip().split(',')
        elif 'set' in tokens:
            columns = query[query.index('set')+len('set'):v_idx].strip().split(',')
        
        
        if 'WHERE' in tokens or 'where' in tokens:
            w_idx = None
            where_idx = tokens.index('WHERE') if 'WHERE' in tokens else tokens.index('where') if 'where' in tokens else None
            if 'WHERE' in tokens:
                w_idx = query.index('WHERE') 
                filters = query[w_idx + len('WHERE'):].strip()
            elif 'where' in tokens: 
                w_idx = query.index('where')
                filters = query[w_idx + len('WHERE'):].strip()
            values = query[v_idx + len("values"): w_idx].strip().split(',')
        else:
            values = query[v_idx + 1:].strip().split(',')

        values = process_list(values)
        columns = [item.strip() for item in columns if item.strip()]

        for col in columns:
            if " " in col:
                return False, {
                    "error": "Invalid query",
                    "message": "Column name has spaces in it. Please enter column names without spaces"
                }

        if columns is None or values is None or len(columns) != len(values):
            return False, {
                "error": "Invalid query",
                "message": "Missing column names or values or number of columns and values do not match"
            }

        return True, {
            'type': 'UPDATE',
            'table_name': table_name,
            'columns': columns,
            'values': values,
            'filters': filters
        }

    elif tokens[0].upper() == 'DELETE':
        if ("delete" not in tokens and "DELETE" not in tokens) or ("FROM" not in tokens and "from" not in tokens):
            return False, {
                "error": "Invalid query",
                "message": "Missing Keywords"
            }
        delete_idx = tokens.index('DELETE') if 'DELETE' in tokens else tokens.index('delete') if 'delete' in tokens else None
        from_idx = tokens.index('FROM') if 'FROM' in tokens else tokens.index('from') if 'from' in tokens else None
        
        if not from_idx:
            return False, {
                "error": "Invalid query",
                "message": "Missing FROM clause"
            }
        table_name = tokens[from_idx + 1]
        if not table_name:
            return False, {
                "error": "Invalid query",
                "message": "Missing table name"
            }
        filters = None
        if 'WHERE' in tokens or 'where' in tokens:
            where_idx = tokens.index('WHERE') if 'WHERE' in tokens else tokens.index('where') if 'where' in tokens else None
            if 'WHERE' in tokens:
                filters = query[query.index('WHERE')+len('WHERE'):].strip()
            elif 'where' in tokens:
                filters = query[query.index('where')+len('where'):].strip()
        return True, {
            'type': 'DELETE',
            'table_name': table_name,
            'filters': filters
        }    

    elif tokens[0].upper() == 'DROP':
        if ("table" not in tokens and "TABLE" not in tokens) or ("drop" not in tokens and "DROP" not in tokens):
            return False, {
                "error": "Invalid query",
                "message": "Missing TABLE clause"
            }
        drop_idx = tokens.index('DROP') if 'DROP' in tokens else tokens.index('drop') if 'drop' in tokens else None
        table_idx = tokens.index('TABLE') if 'TABLE' in tokens else tokens.index('table') if 'table' in tokens else None
        if not table_idx:
            return False, {
                "error": "Invalid query",
                "message": "Missing TABLE clause"
            }
        table_name = tokens[table_idx + 1]
        if not table_name:
            return False, {
                "error": "Invalid query",
                "message": "Missing TABLE name"
            }
        return True, {
            'type': 'DROP',
            'table_name': table_name
        }
        

# query = "CREATE TABLE test column1 int column2 text column3 text PRIMARY_KEY column1 FOREIGN_KEY column2 REFERENCES student column1"
# query = "INSERT INTO test COLUMNS column1, column2, column3 VALUES 1, 'John Doe', 20"
# query = "delete from student where id=1"
# query = "CREATE TABLE test column1 int column2 text column3 text"
# query = "update student set name, age values 'John Doe', 20 where id=1"
# query = "DELETE from student where id == 1 and age == 10"
#query = "SELECT student.id from student INNER_JOIN take ON student.id = take.sid ORDER_BY student.id ASC"
#query="SELECT student.id, test.name from student INNER_JOIN test ON student.id = test.id WHERE student.id == 1 and student.age == 10 or test.name == 1 GROUP_BY student.id ORDER_BY test.id ASC"
# query="SELECT student.id, test.name from student INNER_JOIN test ON sid = id WHERE student.id == 1 and student.age == 10 or test.name == 1 GROUP_BY student.id ORDER_BY test.id ASC"


# print(parse_sql(query))
#result=parse_sql(query)
#print(prettify_json(result))




'''

SELECT student.id, test.name from student INNER_JOIN test ON student.id = test.id WHERE student.id == 1 and student.age == 10 or test.name == "John Doe" GROUP_BY student.id ORDER_BY test.id ASC

{
    'type': 'SELECT',
    'columns': [id],
    'join_columns': [name],
    'table': 'student',
    'join_table': 'test',
    'group_by': id,
    'order_by': id,
    'has_join': True,
    'has_group_by': True,
    'has_order_by': True,
    'has_where': True,
    'order_by_types': 'ASC',
    'group_by_table': 'student',
    'order_by_table': 'test',
    'where': [
        {
            'table_name': 'student',
            'condition': 'id == 1 and age == 20'
        }, {
            'table_name': 'test',
            'condition': 'name == "John Doe"'
        } 
    ],
    'join': [
        {
            'table_name': 'student',
            'column_name': 'id'
        }, {
            'table_name': 'test',
            'column_name': 'id'
        }
    ],
    'condition_between_where': 'or'
}






SELECT student.id, count(test.cno) from student INNER_JOIN test ON student.id = test.id WHERE student.id == 1 and student.age == 10 or test.name == "John Doe" GROUP_BY student.id having count(test.cno) > 10 ORDER_BY test.id ASC

{
    'type': 'SELECT',
    'columns': [student.id, count(test.cno)],
    'table': 'student',
    'join_table': 'test',
    'group_by': student.id,
    'order_by': test.id,
    'has_join': True,
    'has_group_by': True,
    'has_order_by': True,
    'has_having': True,
    'having_clause': 'count(test.cno) > 10',
    'has_where': True,
    'order_by_types': 'ASC',
    'where': 'student.id == 1 and student.age == 10 or test.name == "John Doe"'
    'join': [
        {
            'table_name': 'student',
            'column_name': 'sid'
        }, {
            'table_name': 'test',
            'column_name': 'id'
        }
    ],
}

'''