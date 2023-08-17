# SQL View To Terraform View

import os

# Get the current working directory
current_directory = os.getcwd()

# Specify the relative folder path containing .sql files
relative_folder_path = 'SQL_Files/View'

# Combine the current working directory with the relative folder path
folder_path = os.path.join(current_directory, relative_folder_path)

try:
    # Get a list of all files in the folder
    files = os.listdir(folder_path)

    # Filter out only the .sql files
    sql_files = [file for file in files if file.endswith('.sql')]

    # Read the contents of each .sql file and store them in a list
    sql_contents_list = []
    for sql_file in sql_files:
        file_path = os.path.join(folder_path, sql_file)
        with open(file_path, 'r') as file:
            sql_contents = file.read()
            sql_contents_list.append(sql_contents)
            
except FileNotFoundError:
    print(f"Folder not found: {folder_path}")

except Exception as e:
    print(f"An error occurred: {e}")
    
import re
# this code remove double quotes outside form DDL / Including Database, schema, table name 
def remove_outer_quotes(sql):
    ls1 = sql.split("(")[0].replace('"','')
    ls2 = ["("+i for i in sql.split("(")[1:]] 
    ls2.insert(0,ls1)
    sql = "".join(ls2)  
    
    return sql
resource_table_name_list = []
def python_terraform(sql):
    code = ""
    ddl = sql.split(';')

    for command in ddl: 
        command = command.strip().upper()
        
        # get the Database name,schema name ,table name 
        # Define a regular expression pattern to extract database, schema, and table names
#         info_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?VIEW\s+(?:(?:"?)([A-Z_]+)(?:"?)\.)?(?:"?)([A-Z_]+)(?:"?)\.([A-Z_]+)'
        info_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:(?:"?)([A-Z_]+)(?:"?)\.)?(?:"?)([A-Z_]+)(?:"?)\.([A-Z_]+)'
        
        # Find all matches of the pattern in the SQL code
        info_matches = re.findall(info_pattern, command, re.IGNORECASE)
        # Extract and print the matched database, schema, and table names
        for match in info_matches:
            database_name = match[0].strip('"')
            schema_name = match[1].strip('"')
            table_name = match[2].strip('"')
#             print(f"Database: {database_name}, Schema: {schema_name}, Table: {table_name}")

            data_retention_time_in_days_schema = 1

            # set the dynamic database name  / remove dev , prod name
            dynamic_db = ''
            dynamic__main_db =''
            if database_name.endswith("_DEV"): 
                    dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT_dev}")
                    dynamic__main_db += database_name.replace("_DEV", "")

            elif database_name.endswith("_PROD"):
                    dynamic_db  += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT_prod}")
                    dynamic__main_db += database_name.replace("_PROD", "")
        #------------------------------------------------------------------------------------------------
            
#             value_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?VIEW\s+(?:(?:"?)([A-Z_]+)(?:"?)\.)?(?:"?)([A-Z_]+)(?:"?)\.([A-Z_]+)[\s\S]*?(?<=AS\s)([\s\S]*)$'
            value_pattern = r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SECURE\s+)?(?:MATERIALIZED\s+)?VIEW\s+(?:(?:"?)([A-Z_]+)(?:"?)\.)?(?:"?)([A-Z_]+)(?:"?)\.([A-Z_]+)[\s\S]*?(?<=AS\s)([\s\S]*)$'
            
            value_matches = re.search(value_pattern, sql, re.IGNORECASE | re.DOTALL)

            if value_matches:
                view_definition = value_matches.group(4)
                extracted_code_replaced = re.sub(r'(_PROD|_DEV)', r'_${var.SF_ENVIRONMENT}',view_definition)
            else:
                print("No view definition found.")
            
             # create View  
            value_pattern_M_S = r'CREATE\s+(?:OR\s+REPLACE\s+)?(SECURE\s+)?(MATERIALIZED\s+)?VIEW\s+(?:(?:"?)([A-Z_]+)(?:"?)\.)?(?:"?)([A-Z_]+)(?:"?)\.([A-Z_]+)[\s\S]*?(?<=AS\s)([\s\S]*)$'
            
            value_matches_M_S = re.search(value_pattern_M_S, sql, re.IGNORECASE | re.DOTALL)
              
#             # Print value_matches_M_S to inspect its content


            is_secure = bool(value_matches_M_S.group(1))  # Check if the "SECURE" keyword is captured
            is_materialized = bool(value_matches_M_S.group(2))  # Check if the "MATERIALIZED" keyword is captured
            or_replace_present = bool(re.search(r'\bOR\s+REPLACE\b', sql, re.IGNORECASE | re.DOTALL))  # Check if "OR REPLACE" is present


            if is_secure:
                resource_name_prefix = "snowflake_secure_view"
            elif is_materialized:
                resource_name_prefix = "snowflake_materialized_view"
            else:
                resource_name_prefix = "snowflake_view"

            resource_database_name = f"resource \"{resource_name_prefix}\" \"{dynamic__main_db}_{schema_name}_{table_name}\""

            code += f"{resource_database_name} {{\n"
            code += f"\tdatabase = \"{dynamic_db}\"\n"
            resource_table_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
            resource_table_name_list.append(resource_table_name_demo)
            code += f"\tschema = \"{schema_name}\"\n"
            code += f"\tname = \"{table_name}\"\n"
            code += f"\tdata_retention_time_in_days = {data_retention_time_in_days_schema}\n"

            code += f"\tstatement = <<-SQL \n\t {extracted_code_replaced} \nSQL\n"
            
            if or_replace_present:
                code += f"\tor_replace = true \n"
            else:
                code += f"\tor_replace = false \n"
                
            if is_secure or is_materialized:
                code += f"\tis_secure = true \n"
            else :
                code += f"\tis_secure = false \n"
                
            code += "}\n\n"
            
                
    return code
# Process each SQL content and generate Terraform code
for sql_contents in sql_contents_list:
#     main = python_terraform(sql_contents)
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)


output_folder = os.path.join(current_directory, 'Terraform_Files','View')

try:
    os.makedirs(output_folder, exist_ok=True)
except Exception as e:
    print(f"An error occurred while creating the output folder: {e}")

for i, sql_contents in enumerate(sql_contents_list):
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)

    for i in resource_table_name_list:
        resource_name = i 
        output_filename = os.path.join(output_folder, f"{resource_name}.tf")
     
    try:
        with open(output_filename, 'w') as tf_file:
            tf_file.write(main)
    except Exception as e:
        print(f"An error occurred while writing the output file: {e}")