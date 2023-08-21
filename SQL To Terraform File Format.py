# SQL File Format To Terraform File Format
import os

# Get the current working directory
current_directory = os.getcwd()

# Specify the relative folder path containing .sql files
relative_folder_path = 'SQL_Files/File_Format'

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
resource_File_Format_name_list = []

# main python code 
def python_terraform(sql):
    code = ""
    ddl = sql.split(';')

    for command in ddl:
        command = command.strip().upper()
        create_command = re.findall(r"CREATE(?:\s+OR\s+REPLACE)?\s+FILE FORMAT\s+(.*?)\.(.*?)\.(.*?)\s+", command, re.DOTALL | re.IGNORECASE)

        if create_command:
            database_name, schema_name, table_name = create_command[0]
 
            # set the dynamic database name / remove dev, prod name
            dynamic_db = ''
            dynamic__main_db = ''
            if database_name.endswith("_DEV"):
                dynamic_db += database_name.replace("_DEV", "_${var.SF_ENVIRONMENT}")
                dynamic__main_db += database_name.replace("_DEV", "")
            elif database_name.endswith("_PROD"):
                dynamic_db += database_name.replace("_PROD", "_${var.SF_ENVIRONMENT}")
                dynamic__main_db += database_name.replace("_PROD", "")
            
            # --------------------------------------------------------------------------------------
            # this pattern for Compression
            Compression_match = re.search(r'compression\s*=\s*([\w\d]+)', sql, re.IGNORECASE)
            
            # this pattern for file type
            type_match = re.search(r'type\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)
  
            # this pattern for field delimiter
            field_delimiter_match = re.search(r'field_delimiter\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)

            # this pattern for Encoding
            Encoding_match = re.search(r'encoding\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)

            # this pattern for skip header   
            skip_header_match = re.search(r'skip_header\s*=\s*(\d+)', sql, re.IGNORECASE)
            
            # --------------------------------------------------------------------------------------
            # create File Format  
            resource_File_Format_name = f"resource \"snowflake_file_format\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
            code += f"{resource_File_Format_name} {{\n"
            code += f"\tname = \"{table_name}\"\n"
            code += f"\tdatabase = \"{dynamic_db}\"\n"
            resource_File_Format_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
            resource_File_Format_name_list.append(resource_File_Format_name_demo)
            code += f"\tschema = \"{schema_name}\"\n"
            
            if Compression_match:
                compression_value = Compression_match.group(1)
                code += f"\tcompression = \"{compression_value}\"\n"
            else:
                pass
            
            if Encoding_match:
                encoding_value = Encoding_match.group(1)
                code += f"\tencoding = \"{encoding_value}\"\n"
            else:
                pass
            
            if field_delimiter_match:
                field_delimiter_value = field_delimiter_match.group(1)
                code += f"\tfield_delimiter = \"{field_delimiter_value}\"\n"
            else:
                pass
      
            
            if skip_header_match:
                skip_header_value = skip_header_match.group(1)
                code += f"\tskip_header = {skip_header_value}\n"
            else:
                pass
            
            if type_match:
                type_value = type_match.group(1)    
                code += f"\tformat_type = \"{type_value}\"\n"
            else:
                pass
       
            code += "}\n\n"

    return code


# Process each SQL content and generate Terraform code
for sql_contents in sql_contents_list:
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)

output_folder = os.path.join(current_directory, 'Terraform_Files','File Format')

try:
    os.makedirs(output_folder, exist_ok=True)
except Exception as e:
    print(f"An error occurred while creating the output folder: {e}")

for i, sql_contents in enumerate(sql_contents_list):
    sql_without_quotes = remove_outer_quotes(sql_contents)
    main = python_terraform(sql_without_quotes)

    for i in resource_File_Format_name_list:
        resource_name = i 
        output_filename = os.path.join(output_folder, f"{resource_name}.tf")
     
    try:
        with open(output_filename, 'w') as tf_file:
            tf_file.write(main)
    except Exception as e:
        print(f"An error occurred while writing the output file: {e}")

