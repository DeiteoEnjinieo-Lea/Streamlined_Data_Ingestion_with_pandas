# working with databases are the main tool within DataEngineering
# getting data from a database 

# load pandas and sqlalchemy's create_engine = database connection
import pandas as pd 
from sqlalchemy import create_engine

# create database engine to manage connections 
engine = create_engine("sqlite:///data.db") # the connection to the database and the server

# load entire weather table by table name 
weather = pd.read_sql("weather", engine) # remember the query and the engine 

# OR to load by a physical query in SQL 
weather = pd.read_sql("SELECT * FROM weather", engine) # SELECT ALL query 

# review the results 
print(weather.head())

# the dataset is downloaded please use to create jupyter notebook: data.db in DOWNLOADS 
# Exercise to view the tables within the database 

# Import sqlalchemy's create_engine() function
from sqlalchemy import create_engine

# Create the database engine
engine = create_engine("sqlite:///data.db")

# View the tables in the database
print(engine.table_names())

#Exercise to load the tables into the database 

# Load libraries
import pandas as pd
from sqlalchemy import create_engine

# Create the database engine
engine = create_engine('sqlite:///data.db')

# Load hpd311calls without any SQL
hpd_calls = pd.read_sql("hpd311calls", engine)

# View the first few rows of data
print(hpd_calls.head())

# load second table 
# Create the database engine
engine = create_engine("sqlite:///data.db")

# Create a SQL query to load the entire weather table
query = """
SELECT * FROM weather;
"""

# Load weather with the SQL query
weather = pd.read_sql(query, engine)

# View the first few rows of data
print(weather.head())

# this is how you collaboratively use SQL and pandas; you manage it with python using SQL and pandas

# load libraries 
import pandas as pd
from sqlalchemy import create_engine

# create the database engine 
engine = create_engine("sqlite:///data.db")

# querying our database 
query = """ SELECT * FROM hpd311calls WHERE borough = 'BROOKLYN'; """ # dont forget to execute your clause with semicolon 

# create a execute variable 
bklyn_calls = pd.read_sql(query, engine)

print(bklyn_calls.borough.unique()) # print unique vals

# additional conditions to the query 
and_query = """ SELECT * FROM hpd311calls WHERE borough = 'BRONX' AND complaint_type = 'PLUMBING'; """

bx_plumbing_calls = pd.read_sql(and_query, engine)

# print the record count 
print(bx_plumbing_calls.shape)

# REMINDER AND is when all criteria are met and OR is when one is met 
or_query = """ SELECT * FROM hpd311calls WHERE  complaint_type = 'PLUMBING'AND complaint_type = 'WATER LEAK'; """

# new query var
leaks_or_plumbing = pd.read_sql(or_query,engine)

# recheck record count 
print(leaks_or_plumbing.shape)

# EXERCISE - Selecting columns within SQL 
e# Create database engine for data.db
engine = create_engine("sqlite:///data.db")

# Write query to get date, tmax, and tmin from weather
query = """
SELECT date, 
       tmax, 
       tmin
  FROM weather;
"""

# Make a dataframe by passing query and engine to read_sql()
temperatures = pd.read_sql(query,engine)

# View the resulting dataframe
print(temperatures)

#EXERCISE -- querying the database and plot the results 

# Create query to get hpd311calls records about safety
import pandas as pd

engine = create_engine("sqlite:///data.db")

query = """
SELECT *
FROM hpd311calls
WHERE complaint_type = 'SAFETY';
"""

# Query the database and assign result to safety_calls
safety_calls = pd.read_sql(query,engine)

# Graph the number of safety calls by borough
call_counts = safety_calls.groupby('borough').unique_key.count()
call_counts.plot.barh()
plt.show()

#EXERCISE - Filtering on multi conditions 
import pandas as pd 

engine = create_engine("sqlite:///data.db")

# Create query for records with max temps <= 32 or snow >= 1
query = """
SELECT *
  FROM weather
  WHERE tmax <= 32
  OR  snow >= 1;
"""

# Query database and assign result to wintry_days
wintry_days = pd.read_sql(query, engine)

# View summary stats about the temperatures
print(wintry_days.describe())





