import re
import ipdb

def convert_to_join(condition):
    if(condition==""):
        return ""

    join_condition = [entry.strip() for entry in condition.split('=')]
    if '.' not in join_condition[0] or '.' not in join_condition[1]:
        return ""
    first_parts = join_condition[0].split('.')
    second_parts = join_condition[1].split('.')

    first_table = first_parts[0]
    first_column = first_parts[1]

    second_table = second_parts[0]
    second_column = second_parts[1]

    join_conditions = [
        {'table_name': first_table, 'column_name': first_column},
        {'table_name': second_table, 'column_name': second_column}
    ]

    return join_conditions

def tokenize(query):
    tokens = query.split()
    return tokens

def parse_select_query(sql_query):
    try:
        tokens = tokenize(sql_query)
        if(sql_query==""):
        #print("Invalid Query please Enter Valid Query")
            return False, {
                'message': 'Invalid query'
            }
        
        # Convert the query to lowercase for case insensitivity
        sql_query = sql_query

        #to return if having is before group_by 
        #check_condition = re.compile(r'\bgroup_by\b.*\bhaving\b', re.IGNORECASE | re.DOTALL)
        #validate_have_condition=check_condition.search(sql_query)
        #if(validate_have_condition is None):
        #    return 

        # limit 100
        # select id, name from players limit 1000
        
        # Define regex patterns for different parts of the query
        select_pattern = re.compile(r'select\s(.*?)\sfrom\s(\w+)(?:\swhere|\sgroup_by|\sorder_by|\sINNER_JOIN|\slimit|\Z)', re.IGNORECASE | re.DOTALL)

        #join_pattern = re.compile(r'inner_join(.?on.?)\b(?:where|group_by|order_by|$)', re.DOTALL | re.IGNORECASE)
        join_pattern =re.compile(r'(?<=inner_join\s)(.*?)(?=\bwhere\b|\bgroup_by\b|\bhaving\b|\border_by\b|\slimit|$)', re.IGNORECASE)


        where_pattern = re.compile(r'where(.*?)(group_by|order_by|having|limit|\Z)', re.IGNORECASE | re.DOTALL)
        group_by_pattern=re.compile(r'group_by\s(.*?)(?:having|order_by|limit|\Z)', re.IGNORECASE)
        #order_by_pattern = re.compile(r'order_by(.*?)$', re.IGNORECASE)
        order_by_pattern = re.compile(r'order_by(.*?)\b(?:limit|$)', re.IGNORECASE)
        having_pattern =re.compile(r'having(.*?)(?:order_by|limit|\Z)', re.IGNORECASE)


        
        # Extract information using regex patterns
        select_match = re.search(select_pattern, sql_query)
        join_match = re.search(join_pattern, sql_query)
        where_match = re.search(where_pattern, sql_query)
        group_by_match = re.search(group_by_pattern, sql_query)
        order_by_match = re.search(order_by_pattern, sql_query)
        having_match = re.search(having_pattern, sql_query)
        
        
        
        

        # Initialize result dictionary
        
        result = {'type': 'SELECT'}
        result['has_join']=False
        result['has_where']=False
        result['has_group_by']=False
        result['has_order_by']=False
        result['has_having']=False
        result['join_on']=None
        result['join_table']=None
        result['where_clause']=None
        result['group_by']=None
        result['order_by']=None
        result['order_by_types']=None
        result['having_clause']=None
        result['columns']=None
        result['table']=None
        result['limit']=10000

        if 'limit' in tokens:
            limit_idx = tokens.index('limit')
            result['limit'] = int(tokens[limit_idx + 1])
            


        # Extract columns
        if select_match:
            columns = [col.strip() for col in select_match.group(1).strip().split(',')]
            result['columns'] = columns
            result['table'] = select_match.group(2).strip()


        # Extract table and join information
        if join_match and join_match is not None :
            matched_string =join_match.group(1).strip()
            check_pattern = re.compile(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)')
            match = check_pattern.search(matched_string)
            if match:
                result['join_table']= match.group(3) 
                join_info=[
                    {
                        "table_name": match.group(1),
                        "column_name": match.group(2)
                    },
                    {
                        "table_name": match.group(3),
                        "column_name": match.group(4)
                    }
                ]
                result['join_on'] = join_info
                result['has_join']=True
                
        
        # Extract WHERE clause  
        if where_match and where_match.group(1) is not None:
            result['has_where'] = True
            result['where_clause'] = where_match.group(1).strip()


        # Extract GROUP BY clause
        if group_by_match and group_by_match.group(1) is not None:
            result['has_group_by'] = True
            result['group_by'] = group_by_match.group(1).strip()

        
        #Fixing order by
        '''
        if order_by_match and order_by_match.group(1) is not None:
            result['has_order_by'] = True
            result['order_by_types']='asc'
            order_by_pattern = re.compile(r'order_by\s+(.+?)(?:\s+(asc|desc))?\s*$', re.IGNORECASE)
            reorder_by_match = order_by_pattern.search(order_by_match.group(0))
            if reorder_by_match:
                print(reorder_by_match.group(1).strip())
                capture_pattern = re.compile(r'^(.*?)\s+(?:limit|asc|desc|having|$)', re.IGNORECASE)
                match = capture_pattern.search(reorder_by_match.group(1).strip())
                #print(match.group(1).strip())
                #result['order_by']=reorder_by_match.group(1).strip()
                result['order_by']=match.group(1).strip()
                if(reorder_by_match.group(2) is not None ):
                    print(reorder_by_match.group(1))
                    result['order_by_types']=reorder_by_match.group(2) 
        '''
        #print(order_by_match.group(1))
        keywords = ['having', 'limit']
        keywords_1=['asc','desc']
        if order_by_match and order_by_match.group(1) is not None:
            result['has_order_by'] = True
            result['order_by_types']='asc'
            order_by_pattern = re.compile(r'order_by\s+(.+?)(?:\s+(asc|desc))?\s*$', re.IGNORECASE | re.DOTALL)
            reorder_by_match = order_by_pattern.search(order_by_match.group(0))
            extracted_string=reorder_by_match.group(0).strip()
            contains_keyword = any(keyword in extracted_string.lower() for keyword in keywords)
            if contains_keyword:
                param=True
            else:
                param=False
            if reorder_by_match and param==False:
                if("desc" in extracted_string.lower()):
                    result['order_by']=reorder_by_match.group(1).strip()
                    result['order_by_types']=reorder_by_match.group(2).strip()
                else:
                    result['order_by']=reorder_by_match.group(1).strip()
            if reorder_by_match and param==True:
                capture_pattern = re.compile(r'^(.*?)\s+(?:limit|asc|desc|having|$)', re.IGNORECASE|re.DOTALL)
                match = capture_pattern.search(reorder_by_match.group(1).strip())
                result['order_by']=match.group(1).strip()
                capture_pattern_for_asc_desc = re.compile(r'^(.*?)\s+(?:limit|asc|desc|having|$)', re.IGNORECASE)
                is_asc_dsc_present=reorder_by_match.group(1)
                if "asc" in is_asc_dsc_present:
                    result['order_by_types']="asc"
                if "desc" in is_asc_dsc_present:
                    result['order_by_types']="desc"
        

            
        # Extract HAVING clause
        if having_match and having_match.group(1) is not None:
            result['has_having'] = True
            result['having_clause'] = having_match.group(1).strip()

        return True, result
    except Exception as e:
        print(e)
        return False, {
            'message': f'Failed to parse select query'
        }
    


# sql_query="select student.name, count(*) from student inner_join take on student.id = take.sid order_by name desc limit 100"
# result_dict = parse_select_query(sql_query)
# print(result_dict)



# select student.program, count(take.semester) from student inner_join take on student.id = take.sid where student.age > 20 group_by student.program having count(take.semester) > 1 order_by student.program desc