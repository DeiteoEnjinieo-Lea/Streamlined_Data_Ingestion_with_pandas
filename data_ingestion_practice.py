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










