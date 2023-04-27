# building an data ingestion solution
# pip install dataprep
import pandas as pd 
from dataprep.clean import clean_phone
import os
import shutil

#retrieve data from the source
def extract_data_func():
	df = pd.read_csv()
	return df 

# data transformation
def transform_data_func(df):
	# remove duplicates,blanks 
	# format dates and sort in chronological order 
	# apply a phone_num function to phone_numbers column
	# add cleaned phone_nums to new column
	df.drop_duplicates(inplace=True)
	df.dropna(how='any', subset=['customer_id'])
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df.sort_values(by='date')
    df['phone_num'].astype(str)
    clean_phone(df,'phone_num', inplace=True)
    return df

def load_data_func(df):
    src_file = df.to_csv("c:/folder/anotherfolder/filename.csv", encoding='utf-8')
    # make dir 
    path = "/path/to/new/directory"
    if not os.path.exists(path):
    	os.mkdir(path)
    	print("New directory created:", path)
    else:
    	print("Directory already exists:", path)
    shutil.move(path, "/path/to/another/directory")










import pandas as pd
import boto3

# Load data into a DataFrame
df = pd.read_csv('data.csv')

# Create an S3 client
s3 = boto3.client('s3')

# Create a new S3 bucket
bucket_name = 'my-new-bucket'
s3.create_bucket(Bucket=bucket_name)

# Upload the file to the bucket
s3.upload_file('data.csv', bucket_name, 'new_directory/data.csv')



# make dir 
import os

path = "/path/to/new/directory"

if not os.path.exists(path):
    os.mkdir(path)
    print("New directory created:", path)
else:
    print("Directory already exists:", path)

# this data ingestion done by You chat 
import pandas as pd
import os

# Ingest data using pandas
df = pd.read_csv('input_data.csv')

# Transform data
df['new_column'] = df['existing_column'] * 2

# Create new directory for output file
output_directory = 'output_data'
if not os.path.exists(output_directory):
    os.mkdir(output_directory)

# Write transformed data to output file in new directory
output_file_path = os.path.join(output_directory, 'output_data.csv')
df.to_csv(output_file_path, index=False)

print(f"Transformed data saved to {output_file_path}")

# ELT = Extract Load Transform not ETL with PETL works better 
import petl as etl

# Extract data from a CSV file
table1 = etl.fromcsv('input_data.csv')

# Load data into a SQLite database
db = etl.connect("sqlite:///output.db")
etl.todb(table1, db, "table1")

# Transform data
table2 = etl.cut(table1, 'column1', 'column2', 'new_column', 'column3')
table3 = etl.rename(table2, {'column1': 'new_name1', 'column2': 'new_name2'})

# Load transformed data into the same database
etl.todb(table3, db, "table2")

# Write final results to a CSV file
etl.tocsv(table3, 'output_data.csv')


