import os
import zipfile
import json
import pandas as pd
import shutil

# Get the information of the files. Make sure you set the encoding to utf-16-le, otherwise the data can't be read from the DataModelSchema files.
def extract_source_info(zip_path, encoding='utf-16-le'):
    temp_extract_dir = "temp_extracted" # Creates the temp folder to store the extracted data to get the DataModelSchema file.
    os.makedirs(temp_extract_dir, exist_ok=True)  # Ensures the directory exists. If it doesn't we don't continue.
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref: # Opens the zip file in read mode.
        # Extracts files individually to handle potential long path issues.
        for file_info in zip_ref.infolist(): # Goes through the files contained in the zipped archive.
            # Construct the target path for the file
            long_target_path = "\\\\?\\" + os.path.abspath(os.path.join(temp_extract_dir, file_info.filename)) # Used "\\\\?\\" to handle potential long file names.
            # Ensure that the directory exists for the file to be extracted into.
            os.makedirs(os.path.dirname(long_target_path), exist_ok=True)
            # Extracts the file with shutil.
            with zip_ref.open(file_info) as source:
                with open(long_target_path, "wb") as target:
                    shutil.copyfileobj(source, target)
        
        # Now, the files have been extracted, continue with loading the DataModelSchema. Also checks if the file exists.
        schema_path = os.path.join(temp_extract_dir, "DataModelSchema")
        if not os.path.exists(schema_path):
            print(f"DataModelSchema file not found in {zip_path}") # If the file doesn't exist, displays an error.
            return []
        
        with open(schema_path, "r", encoding=encoding) as file: # Opens the DataModelSchema file with the specific encoding.
            data_model = json.load(file) # Loadd the JSON content from the DataModelSchema file into a dictionary.
            # Navigate to the 'model' key in the JSON structure and then to 'tables' to find 'partitions'.
            if 'model' in data_model and 'tables' in data_model['model']:
                tables = data_model['model']['tables']
                # Flatten the list of partitions from all tables.
                sources = [partition for table in tables if 'partitions' in table for partition in table['partitions']]
                # Debugging: print the sources to see if they are being extracted correctly.
                print(f"Extracted sources from {zip_path}:")
                print(sources)
                return sources
            else:
                print(f"No 'model' or 'tables' key found in {zip_path}")
                return []
# Defines a function to rename .pbit files to .zip, extract source information, and collect this information in a list.
def rename_and_extract_sources(directory):
    extracted_info = [] # Initializes an empty list to hold the extracted information.
    for filename in os.listdir(directory): # Iterates over all files in the specified directory.
        if filename.endswith(".pbit"): #C hecks if the file has a .pbit extension.
            # Renames the .pbit file to .zip.
            base = os.path.splitext(filename)[0]
            zip_path = os.path.join(directory, base + ".zip")
            os.rename(os.path.join(directory, filename), zip_path)
            sources = extract_source_info(zip_path)
            # Debugging: print the sources to see if the list is populated.
            print(f"Sources for {filename}:")
            print(sources)
            for source in sources:
                source_info = {
                    "File Name": base,
                    **source
                }
                # Debugging: print the source_info before appending.
                print(f"Source info to append for {filename}:")
                print(source_info)
                extracted_info.append(source_info)
    # Debugging: print the final extracted_info to see if it's correct.
    print("Final extracted information:")
    print(extracted_info)
    return extracted_info

# Saves the needed information from .pbit and the generated .zip files into an Excel spreadsheet.
def save_to_excel(extracted_info, output_file):
    df = pd.DataFrame(extracted_info) # Saves the DataFrame to an Excel file without the index column.
    # Debugging: print the DataFrame to ensure it's not empty.
    print("DataFrame to be saved to Excel:")
    print(df)
    df.to_excel(output_file, index=False)

# These lines set the directory to process, call the function to rename and extract sources, and then save the extracted information to an Excel file named output.xlsx.
directory = "C:/Users/YourDirectory"
extracted_info = rename_and_extract_sources(directory)
save_to_excel(extracted_info, "output.xlsx")
