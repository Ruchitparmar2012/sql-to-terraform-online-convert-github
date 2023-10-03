

import re
# this code remove double quotes outside form DDL / Including Database, schema, table name 
def remove_outer_quotes(sql):
    ls1 = sql.split("(")[0].replace('"','')
    ls2 = ["("+i for i in sql.split("(")[1:]] 
    ls2.insert(0,ls1)
    sql = "".join(ls2)  
    
    return sql


import re
resource_table_name_list=  []
def python_terraform(sql):
    code = ""
    
    ddl = sql.split(';')
    # create a main loop for find the all data  
    
    for command in ddl: 
        command = command.strip().upper()
#         command = command.replace('CREATE', 'CREATE OR REPLACE') 
#         if command.startswith("CREATE "):
            
        # Updated regex for SQL functions
        create_commands = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION(.*?)\(", command, re.DOTALL)
        
        # get the database name, schema name, function name
        for create_command in create_commands:
            create_command = create_command.strip()
            # get the database name, schema name, function name
            extract_schema_database_function = re.search(r'\b(\w+)\.(\w+)\.(\w+)', create_command)
            database_name, schema_name, table_name = extract_schema_database_function.groups()
  
            
            data_retention_time_in_days_schema = 1

            # set the dynamic database name  / remove dev , prod name
            dynamic_db = ''
            dynamic__main_db =''
            if database_name.endswith("_DEV"): 
                    dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_DEV", "")

            elif database_name.endswith("_PROD"):
                    dynamic_db  += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                    dynamic__main_db += database_name.replace("_PROD", "")
            
            
            #----------------------------------------------------------------------------------


            #------------------------------------------------------------------------------------------------
            # All regex Pattern

            return_pattern = r'RETURNS\s+(.*?)\n\s*'

            return_type_values = re.findall(return_pattern, sql, re.DOTALL | re.IGNORECASE)

            return_type_value = return_type_values[0].strip() if return_type_values else None

            arguments_match = re.findall(r'"([^"]+)"\s+([A-Z][A-Z0-9()]*(?:\([0-9, ]+\))?)(?:,\s*|\))', sql)
            arguments = [{"name": arg[0], "type": arg[1]} for arg in arguments_match]

            # regex for language
            language_match = re.search(r'LANGUAGE\s+["\']?(\w+)["\']?', sql, re.IGNORECASE)

            if language_match:
                language_value = language_match.group(1)

            #regex for null_input_behavior
            null_input_behavior = re.search(r'NULL_INPUT_BEHAVIOR\s+([^\n]+)', sql ,re.DOTALL | re.IGNORECASE)

            if null_input_behavior:
                null_input_behavior=null_input_behavior.group(1).strip()

            # regex for return_behavior
            return_behavior_item = re.search(r'return_behavior\s+([^\n]+)', sql, re.DOTALL | re.IGNORECASE)

            if return_behavior_item:
                return_behavior_value = return_behavior_item.group(1).strip()
  

            # regex for comemnt
            comment_match = re.search(r'COMMENT\s+["\'](.+?)["\']', sql, re.DOTALL | re.IGNORECASE)
            if comment_match:
                    comment_got = comment_match.group(1).strip()

            
            # Use re.search() to find the first occurrence of the pattern in the SQL code
            statemen_match = re.search(r"(AS '(?:[^']|''|[^;])*';)", sql, re.DOTALL)
            
            # Check if there's a match before accessing the group attribute
            if statemen_match:
                statement_matches = statemen_match.group(0).strip("AS '").strip("';")
                extracted_code_single_quotes = statement_matches.replace("''", "'")
                extracted_code_replaced = re.sub(r'(_PROD|_DEV)', r'_${var.SF_ENVIRONMENT}', extracted_code_single_quotes)
            else:
                pass
                
            sql = sql.upper()

            

            # main code 
            resource_table_name = f"resource \"snowflake_procedure\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
            code += f"{resource_table_name} {{\n"
            code += f"\tname = \"{table_name}\"\n"
            code += f"\tdatabase = \"{dynamic_db}\"\n"
            code += f"\tschema = \"{schema_name}\"\n"
            resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
            resource_table_name_list.append(resource_table_name_demo)
            code += f"\tlanguage  = \"{language_value}\"\n"

            argument_name = []
            argument_type = []
            for argument in arguments:

                    argument_name.append(argument["name"])
                    argument_type.append(argument["type"])

            for i in zip(argument_name,argument_type):
                code += f"\n\targuments {{\n"
                code += f"\t\tname = \"{i[0]}\"\n"
                code += f"\t\ttype = \"{i[1]}\"\n"
                code += "}\t\n"

            if "COMMENT" in sql:
                try:
                        code += f"\tcomment = \"{comment_got}\"\n"
                except AttributeError:
                    pass
            elif "COMMENT" not in sql:
                pass
                
            if "RETURNS" in sql:
                try:
                    return_type_value = return_type_value.replace('"','')
                    code += f"\treturn_type = \"{return_type_value}\"\n"
                    
                except AttributeError:
                    pass
            elif "RETURNS" not in sql:
                pass  

            if "NULL_INPUT_BEHAVIOR" in sql:
                try:
                    null_input_behavior = null_input_behavior.replace('"','')
                    code += f"\tnull_input_behavior  = \"{null_input_behavior}\"\n"
                except AttributeError:
                    pass
            elif "NULL_INPUT_BEHAVIOR" not in sql:
                pass
                
            if "RETURN_BEHAVIOR" in sql:
                try:
                    return_behavior_value = return_behavior_value.replace('"','')
                    code += f"\treturn_behavior = \"{return_behavior_value}\"\n"
                except AttributeError:
                    pass
            elif "RETURN_BEHAVIOR" not in sql:
                pass

            code += f"\tstatement =  \"{extracted_code_replaced}\"\n"

            code += "}\n\n"

           
                
    return code


sql = '''
CREATE OR REPLACE FUNCTION DISC_PROD.DATA_CLEANSING.SSN_VALIDATION("STR_ETL_TASK_KEY" VARCHAR(16777216), "STR_CDC_START" VARCHAR(16777216), "STR_CDC_END" VARCHAR(16777216))
RETURNS VARCHAR(12)
COMMENT 'demoaa'
LANGUAGE "SQL"
NULL_INPUT_BEHAVIOR 111
return_behavior number(1110)
AS '
     SELECT 
     CASE 
    WHEN TRIM(SSN) = ' OR SSN IS NULL THEN  NULL
    WHEN LENGTH(REGEXP_REPLACE(TRIM(SSN),'[^[:digit:]$]'))<>9 THEN NULL
        WHEN NOT (NULLIF(TRIM(REGEXP_REPLACE(SSN ,'\\-|\\\\\\\\s|\\\\\\\\\\\\\\\\|[A-Z]')),') LIKE ANY ('666%','000%','9%','%0000','___00%'))
        THEN (CASE WHEN
     CONTAINS(TRIM(SSN), '-') = FALSE  THEN CONCAT(SUBSTRING(SSN,1,3)||''-''||SUBSTRING(SSN,4,2)||'-'||SUBSTRING(SSN,6,4))
     ELSE TRIM(SSN)
     END)
        ELSE  NULL 
    END
';
'''
sql_without_quotes = remove_outer_quotes(sql)
main = python_terraform(sql_without_quotes)
print(main)

