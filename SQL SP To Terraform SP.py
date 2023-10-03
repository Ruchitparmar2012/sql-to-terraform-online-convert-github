import os
import re
import time  # Import the time module

# Get the current time in microseconds before starting execution
start_time = time.perf_counter()

# Get the current time in microseconds after finishing execution
end_time = time.perf_counter()

# Calculate the elapsed time in microseconds
elapsed_time_microseconds = (end_time - start_time) * 1e6  # Convert to microseconds

# Print the elapsed time in microseconds
print(f"Elapsed time: {elapsed_time_microseconds:.2f} microseconds")

# Get the current working directory
current_directory = os.getcwd()

# Specify the relative folder path containing .sql files
relative_folder_path = 'SQL_Files/stored procedure'

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
