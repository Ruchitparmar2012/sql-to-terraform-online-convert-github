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
            # type_match = re.search(r'type\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)
            type_match = re.search(r'type\s*=\s*[\'"]?([^\n\'"]+)[\'"]?', sql, re.IGNORECASE)

            
            # this pattern for field delimiter

            field_delimiter_match = re.search(r'field_delimiter\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)

            # this pattern for Encoding
            Encoding_match = re.search(r'encoding\s*=\s*\'([^\']+)\'', sql, re.IGNORECASE)

                
            # this pattern for skip header   
            skip_header_match = re.search(r'skip_header\s*=\s*(\d+)', sql, re.IGNORECASE)

            # this pattern for ignore_utf8_errors   
            ignore_utf8_errors_match = re.search(r'ignore_utf8_errors\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for preserve_space   
            preserve_space_match = re.search(r'preserve_space\s*=\s*(\w+)', sql, re.IGNORECASE)
            
            # this pattern for strip outer element   
            strip_outer_element_match = re.search(r'strip_outer_element\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for disable_snowflake_data   
            disable_snowflake_data_match = re.search(r'disable_snowflake_data\s*=\s*(\w+)', sql, re.IGNORECASE)
            
            # this pattern for disable_auto_convert   
            disable_auto_convert_match = re.search(r'disable_auto_convert\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for replace_invalid_characters   
            replace_invalid_characters_match = re.search(r'replace_invalid_characters\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for skip_byte_order_mark   
            skip_byte_order_mark_match = re.search(r'skip_byte_order_mark\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for date_format   
            date_format_match = re.search(r'\bdate_format\s*=\s*(\'[^\']*\'|AUTO)\s*', sql, re.IGNORECASE)
            # this pattern for time_format   
            time_format_match = re.search(r'\btime_format\s*=\s*(\'[^\']*\'|AUTO)\s*', sql, re.IGNORECASE)

            # this pattern for timestamp_format   
            timestamp_format_match = re.search(r'\btimestamp_format\s*=\s*(\'[^\']*\'|AUTO)\s*', sql, re.IGNORECASE)

            # this pattern for binary_format   
            binary_format_match = re.search(r'\bbinary_format\s*=\s*(\'[^\']*\'|[A-Za-z0-9_]+)\s*', sql, re.IGNORECASE)

            # this pattern for trim_space   
            trim_space_match = re.search(r'\btrim_space\s*=\s*(TRUE|FALSE)\s*', sql, re.IGNORECASE)

            # this pattern for file_extension   
            file_extension_match = re.search(r'\bfile_extension\s*=\s*(\'[^\']*\'|\.[A-Za-z0-9_]+)\s*', sql, re.IGNORECASE)

            # this pattern for enable_octal   
            enable_octal_match = re.search(r'enable_octal\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for allow_deplicate   
            allow_deplicate_match = re.search(r'allow_duplicate\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for strip_outer_array   
            strip_outer_array_match = re.search(r'strip_outer_array\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for strip_null_values   
            strip_null_values_match = re.search(r'strip_null_values\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for replace_invalid_characters   
            replace_invalid_characters_match = re.search(r'replace_invalid_characters\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for null_if   
            null_if_match = re.search(r'\bnull_if\s*=\s*(\'[^\']*\'|AUTO)\s*', sql, re.IGNORECASE)

            # this pattern for parse_header   
            parse_header_match = re.search(r'parse_header\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for skip_blank_lines
            skip_blank_lines_match = re.search(r'skip_blank_lines\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for error_on_column_count_mismatch
            error_on_column_count_mismatch_match = re.search(r'error_on_column_count_mismatch\s*=\s*(\w+)', sql, re.IGNORECASE)

            # this pattern for replace_invalid_characters
            replace_invalid_characters_match = re.search(r'replace_invalid_characters\s*=\s*(\w+)', sql, re.IGNORECASE)
            
            # this pattern for empty_field_as_null
            empty_field_as_null_match = re.search(r'empty_field_as_null\s*=\s*(\w+)', sql, re.IGNORECASE)
            
            # this pattern for escape
            escape_matches = re.findall(r"escape\s*=\s*(?:'([^']*)'|([^;\n]*))", sql, re.IGNORECASE)
            
            # this pattern for enclosed_by
            field_optionally_enclosed_by_matches = re.findall(r"field_optionally_enclosed_by\s*=\s*(?:'([^']*)'|([^;\n]*))", sql, re.IGNORECASE)

            # this pattern for record_delimiter
            record_delimiter_match = re.search(r"record_delimiter\s*=\s*('([^']*)'|None)", sql ,re.IGNORECASE)
            
            # this pattern for escape_unenclosed_field
            escape_unenclosed_field_matches = re.findall(r"escape_unenclosed_field\s*=\s*(?:'([^']*)'|([^;\n]*))", sql, re.IGNORECASE)
    

            # --------------------------------------------------------------------------------------
            # create File Format  
            resource_File_Format_name = f"resource \"snowflake_file_format\" \"{dynamic__main_db}_{schema_name}_{table_name}\""
            code += f"{resource_File_Format_name} {{\n"
            code += f"\tname = \"{table_name}\"\n"
            code += f"\tdatabase = \"{dynamic_db}\"\n"
            resource_File_Format_name_demo = f'{dynamic__main_db}_{schema_name}_{table_name}'
            resource_File_Format_name_list.append(resource_File_Format_name_demo)
            code += f"\tschema = \"{schema_name}\"\n"

            
            if type_match:
                type_value = type_match.group(1).lower()
                if type_value == 'csv':
                    if type_match:
                        type_value = type_match.group(1)    
                        code += f"\tformat_type = \"{type_value}\"\n"
                    else:
                        pass
            
                    if Compression_match:
                        compression_value = Compression_match.group(1)
                        code += f"\tcompression = {compression_value}\n"
                    else:
                        code += f"\tcompression = AUTO\n"
                        
                    # Check if a match was found
                    if record_delimiter_match:
                        record_delimiter = record_delimiter_match.group(1)
                        if record_delimiter.lower() == 'none':
#                             print(repr(record_delimiter)[1:-1])
                            code += f"\trecord_delimiter = NONE\n"
                        else :
                            code += f"\trecord_delimiter = \"{repr(record_delimiter)[2:-2]}\"\n"
                    else:
                        code += f"\trecord_delimiter = NONE\n"
                    
                    if field_delimiter_match:
                        field_delimiter_value = field_delimiter_match.group(1)
                        code += f"\tfield_delimiter = \"{field_delimiter_value}\"\n"
                    else:
                        code += f"\tfield_delimiter = NONE\n"
                        
                    if file_extension_match:
                        file_extension_value = file_extension_match.group(1)
                        file_extension_value = file_extension_value.strip("'")
                        code += f"\tfile_extension = \"{file_extension_value}\"\n"
                    else:
                        code += f"\tfile_extension = NONE\n"
                        
                    if parse_header_match:
                        parse_header_value = parse_header_match.group(1).lower()
                        code += f"\tparse_header = {parse_header_value}\n"                
                    else:
                        code += f"\tparse_header =  false\n"
                        
                    if skip_header_match:
                        skip_header_value = skip_header_match.group(1)
                        code += f"\tskip_header = {skip_header_value}\n"
                    else:
                        code += f"\tskip_header = 0\n"
                        
                    if skip_blank_lines_match:
                        skip_blank_lines_value = skip_blank_lines_match.group(1).lower()
                        code += f"\tskip_blank_lines = {skip_blank_lines_value} \n"                
                    else:
                        code += f"\tskip_blank_lines = false\n"
                        
                    if date_format_match:
                        date_format_value = date_format_match.group(1)
                        date_format_value = date_format_value.strip("'")
                        code += f"\tdate_format = \"{date_format_value}\"\n"                
                    else:
                        code += f"\tdate_format = AUTO\n"
                        
                    if time_format_match:
                        time_format_value = time_format_match.group(1)
                        time_format_value = time_format_value.strip("'")
                        code += f"\ttime_format = \"{time_format_value}\"\n"
                    else:
                        code += f"\tdate_format = AUTO\n"

                    if timestamp_format_match:
                        timestamp_format_value = timestamp_format_match.group(1)
                        timestamp_format_value = timestamp_format_value.strip("'")
                        code += f"\ttimestamp_format = \"{timestamp_format_value}\"\n"
                    else:
                        code += f"\ttimestamp_format = AUTO\n"

                    if binary_format_match:
                        binary_format_value = binary_format_match.group(1)
                        code += f"\tbinary_format = {binary_format_value}\n"
                    else:
                        code += f"\tbinary_format = HEX\n"
                    
                    if escape_matches:
                        escape_value = escape_matches[0][0] or escape_matches[0][1]
                        if escape_value in '"':
                            demo =  '\"'
                            code += f"\tescape_ = \"\{demo}\"\n"  
                        else:
                            code += f"\tescape = \"{escape_value}\"\n"                
                    else:
                        code += f"\tescape = NONE\n" 
                    
                    if escape_unenclosed_field_matches:
                        escape_unenclosed_field_value = escape_unenclosed_field_matches[0][0] or escape_unenclosed_field_matches[0][1]
                        if escape_unenclosed_field_value in '"':
                            demo =  '\"'
                            code += f"\tescape_unenclosed_field = \"\{demo}\"\n"  
                        else:
                            code += f"\tescape_unenclosed_field = \"{escape_unenclosed_field_value}\"\n"  
                    else:
                         demo = "\\\\"
                         code += f"\ttescape_unenclosed_field = {demo}\n" 


                    if trim_space_match:
                        trim_space_value = trim_space_match.group(1).lower()
                        code += f"\ttrim_space = {trim_space_value}\n"
                    else:
                        code += f"\ttrim_space = FALSE\n"
                        
                    if field_optionally_enclosed_by_matches:
                        field_optionally_enclosed_by_value = field_optionally_enclosed_by_matches[0][0] or field_optionally_enclosed_by_matches[0][1]
                        if field_optionally_enclosed_by_value in '"':
                            demo =  '\"'
                            code += f"\tfield_optionally_enclosed_by = \"\{demo}\"\n"  
                        else:
                            code += f"\tfield_optionally_enclosed_by = \"{field_optionally_enclosed_by_value}\"\n"  
                    else:
                        code += f"\tfield_optionally_enclosed_by = NONE\n" 
                        
                    if null_if_match:
                        null_if_value = null_if_match.group(1)
                        null_if_value = null_if_value.strip("'")
                        code += f"\tnull_if = \"{null_if_value}\"\n"                
                    else:
                        null_if_value_Default = "\\n"
                        code += f"\tnull_if = \"\{null_if_value_Default}\"\n" 
                    
                    if error_on_column_count_mismatch_match:
                        error_on_column_count_mismatch_value = error_on_column_count_mismatch_match.group(1).lower()
                        code += f"\terror_on_column_count_mismatch = {error_on_column_count_mismatch_value}\n"                
                    else:
                        code += f"\terror_on_column_count_mismatch_ = true\n"   
                        
                    if replace_invalid_characters_match:
                        replace_invalid_characters_value = replace_invalid_characters_match.group(1).lower()
                        code += f"\treplace_invalid_characters = {replace_invalid_characters_value}\n"
                    else:
                        code += f"\treplace_invalid_characters = false\n"
                        
                    if empty_field_as_null_match:
                        empty_field_as_null_value = empty_field_as_null_match.group(1).lower()
                        code += f"\tempty_field_as_null = {empty_field_as_null_value}\n"                
                    else:
                        code += f"\tempty_field_as_null = true\n" 
                        
                    if skip_byte_order_mark_match:
                        skip_byte_order_mark_value = skip_byte_order_mark_match.group(1).lower()
                        code += f"\tskip_byte_order_mark = {skip_byte_order_mark_value}\n"
                    else:
                        code += f"\tskip_byte_order_mark = true\n"
                    
                    if Encoding_match:
                        encoding_value = Encoding_match.group(1)
                        code += f"\tencoding = \"{encoding_value}\"\n"
                    else:
                        code += f"\tencoding = UTF8\n"
## -----------------------------------------------------------------------------------------------------------------------                
                ### this is  for XML
                if type_value == 'xml':
                    if type_match:
                        type_value = type_match.group(1)    
                        code += f"\tformat_type = \"{type_value}\"\n"
                    else:
                        pass
                    if Compression_match:
                        compression_value = Compression_match.group(1)
                        code += f"\tcompression = {compression_value}\n"
                    else:
                        code += f"\tcompression = AUTO\n"
                    
                    if ignore_utf8_errors_match:
                        ignore_utf8_errors_value = ignore_utf8_errors_match.group(1).lower()
                        code += f"\tignore_utf8_errors = {ignore_utf8_errors_value}\n"
                    else:
                        code += f"\tignore_utf8_errors = false\n"
                    
                    if preserve_space_match:
                        preserve_space_value = preserve_space_match.group(1).lower()
                        code += f"\tpreserve_space = {preserve_space_value}\n"
                    else:
                        code += f"\tpreserve_space = false\n"
                
                    if strip_outer_element_match:
                        strip_outer_element_value = strip_outer_element_match.group(1).lower()
                        code += f"\tstrip_outer_element = {strip_outer_element_value}\n"
                    else:
                        code += f"\tstrip_outer_element = false\n"
                
                    if disable_snowflake_data_match:
                        disable_snowflake_data_value = disable_snowflake_data_match.group(1).lower()
                        code += f"\tdisable_snowflake_data = {disable_snowflake_data_value}\n"
                    else:
                        code += f"\tdisable_snowflake_data = false\n"

                    if disable_auto_convert_match:
                        disable_auto_convert_value = disable_auto_convert_match.group(1).lower()
                        code += f"\tdisable_auto_convert = {disable_auto_convert_value}\n"
                    else:
                        code += f"\tdisable_auto_convert = false\n"

                    if replace_invalid_characters_match:
                        replace_invalid_characters_value = replace_invalid_characters_match.group(1).lower()
                        code += f"\treplace_invalid_characters = {replace_invalid_characters_value}\n"
                    else:
                        code += f"\treplace_invalid_characters = false\n"

                    if skip_byte_order_mark_match:
                        skip_byte_order_mark_value = skip_byte_order_mark_match.group(1).lower()
                        code += f"\tskip_byte_order_mark = {skip_byte_order_mark_value}\n"
                    else:
                        code += f"\tskip_byte_order_mark = true\n"
## -----------------------------------------------------------------------------------------------------------------------                
                 ### this is  for json
                if type_value == 'json':
                    if type_match:
                        type_value = type_match.group(1)    
                        code += f"\tformat_type = \"{type_value}\"\n"
                    else:
                        pass
                    if Compression_match:
                        compression_value = Compression_match.group(1)
                        code += f"\tcompression = {compression_value}\n"
                    else:
                        code += f"\tcompression = AUTO\n"
                        
                    if date_format_match:
                        date_format_value = date_format_match.group(1)
                        date_format_value = date_format_value.strip("'")
                        code += f"\tdate_format = \"{date_format_value}\"\n"                
                    else:
                        code += f"\tdate_format = AUTO\n"
                        
                    if time_format_match:
                        time_format_value = time_format_match.group(1)
                        time_format_value = time_format_value.strip("'")
                        code += f"\ttime_format = \"{time_format_value}\"\n"
                    else:
                        code += f"\tdate_format = AUTO\n"

                    if timestamp_format_match:
                        timestamp_format_value = timestamp_format_match.group(1)
                        timestamp_format_value = timestamp_format_value.strip("'")
                        code += f"\ttimestamp_format = \"{timestamp_format_value}\"\n"
                    else:
                        code += f"\ttimestamp_format = AUTO\n"

                    if binary_format_match:
                        binary_format_value = binary_format_match.group(1)
                        code += f"\tbinary_format = {binary_format_value}\n"
                    else:
                        code += f"\tbinary_format = HEX\n"
                                    
                    if trim_space_match:
                        trim_space_value = trim_space_match.group(1).lower()
                        code += f"\ttrim_space = {trim_space_value}\n"
                    else:
                        code += f"\ttrim_space = false\n"
                        
                    if null_if_match:
                        null_if_value = null_if_match.group(1)
                        null_if_value = null_if_value.strip("'")
                        code += f"\tnull_if = \"{null_if_value}\"\n"                
                    else:
                        null_if_value_Default = "\\n"
                        code += f"\tnull_if = \"\{null_if_value_Default}\"\n" 
                    
                    if file_extension_match:
                        file_extension_value = file_extension_match.group(1)
                        file_extension_value = file_extension_value.strip("'")
                        code += f"\tfile_extension = \"{file_extension_value}\"\n"
                    else:
                        code += f"\tfile_extension = NONE\n" 
                        
                    if enable_octal_match:
                        enable_octal_value = enable_octal_match.group(1).lower()
                        code += f"\tenable_octal = {enable_octal_value}\n"                
                    else:
                        code += f"\tenable_octal = false\n"
                    
                    if allow_deplicate_match:
                        allow_deplicate_value = allow_deplicate_match.group(1).lower()
                        code += f"\tallow_deplicate = {allow_deplicate_value}\n"                

                    else:
                        code += f"\tallow_deplicate = false\n"  
                    
                    if strip_outer_array_match:
                        strip_outer_array_value = strip_outer_array_match.group(1).lower()
                        code += f"\tallow_deplicate = {allow_deplicate_value}\n"                
                    else:
                        code += f"\tallow_deplicate = false\n"              

                    if strip_null_values_match:
                        strip_null_values_value = strip_null_values_match.group(1).lower()
                        code += f"\tstrip_null = {strip_null_values_value}\n"                
                    else:
                        code += f"\tstrip_null = false\n"                

                    if replace_invalid_characters_match:
                        replace_invalid_characters_value = replace_invalid_characters_match.group(1).lower()
                        code += f"\treplace_invalid_characters = {replace_invalid_characters_value}\n"                
                    else:
                        code += f"\treplace_invalid_characters= false\n"  
                    
                    if ignore_utf8_errors_match:
                        ignore_utf8_errors_value = ignore_utf8_errors_match.group(1).lower()
                        code += f"\tignore_utf8_errors = {ignore_utf8_errors_value}\n"
                    else:
                        code += f"\tignore_utf8_errors = false\n"
                    
                    if skip_byte_order_mark_match:
                        skip_byte_order_mark_value = skip_byte_order_mark_match.group(1).lower()
                        code += f"\tskip_byte_order_mark = {skip_byte_order_mark_value}\n"
                    else:
                        code += f"\tskip_byte_order_mark = true\n"
                
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
