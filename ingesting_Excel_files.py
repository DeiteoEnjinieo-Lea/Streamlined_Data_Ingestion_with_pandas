# using Excel for data ingestion, 
# spreadsheets are the main data storage within organizations 
# esp for those on the less technical side 

#example 
import pandas as pd 
URL: r'/path/folder/to/dir/excelfilename.xlsx'

# Read the excel file 
excel_src = pd.read_excel(URL)

#print
print(excel_src.head())

# see below 
survery_data = pd.read_excel("fcc_survey.xlsx")

# view the first 5 lines of data 
print(survery_data.head())

# sometimes you need to select columns and rows; metadata headers or invoices

#read columns W-AB and AR of the file, skipping metadata header 
survery_data = pd.read_excel("fcc_survey_with_headers.xlsx", skiprows=2, usecols="W:AB, AR")

#check our query filter 
print(survery_data.head())

# create a dataframe from an Excel file: one with a single sheet of tabular data

#load pandas lib 
import pandas as pd 

# read the spreadsheet and assign it 2 a variable 
survey_responses = pd.read_excel("fcc_survey.xlsx")

# view the head of the dataframe 
print(survey_responses.head())

# if you need to load a piece of a spreadsheet from a specific column range

# Create string of lettered columns to load
col_string = "AD, AW:BA"

# Load data with skiprows and usecols set
survey_responses = pd.read_excel("fcc_survey_headers.xlsx", 
                        skiprows=2, 
                        usecols=col_string)

# View the names of the columns selected
print(survey_responses.columns)

# data ingestion with mutiple worksheets 
# the file excel = notebook, tabular form = sheets (read_excel() loads the 1st sheet in a Excel workbook by default)
# use sheet_name to specify where your pulling from within the notebook 

# Get the second sheet by position index 
survery_data_sheet2 = pd.read_excel('fcc_survey.xlsx', sheet_name=1)

# OR by sheet_name 
survery_data_sheet2 = pd.read_excel('fcc_survey.xlsx', sheet_name='2017')

# which you can data check and verify they match with the equals fx 
print(survery_data_sheet2.equals(survery_data_2017))

# to pull all the sheets within a workbook; pass None to sheet_name to read everything 
survey_responses = pd.read_excel("fcc_survey.xlsx", sheet_name=None)

#check the type
print(type(survey_responses))

# iterate aka loop thru the keys = sheet names 
for key, value in survey_responses.item():
	print(key, type(value))


# FOR GITHUB:put it all together -- README.md This is a data ingestion assignment importing excel files from the DataCamp course Streamlined Data Ingestion with Pandas
# modifications were made with data cleansing with Excel and then ingesting the workbook thru pandas so TEL (Transform, Extract and Load) 
#ibraries used: pandas

#create a empty df to hold all loaded sheets 
all_responses = pd.DataFrame()

# iterate through df in dict 
for sheet_name, frame in survey_responses.items():
	# add a col so we know which year data is from 
	frame["year"] = sheet_name
	# each key is a sheet name

	# add each df to all_responses
	all_responses = all_responses.append(frame)

# view years in data 
print(all_responses.Year.unique())


# SINGLE SHEET
# Create df from second worksheet by referencing its position
responses_2017 = pd.read_excel("fcc_survey.xlsx",
                               sheet_name=1)

# Graph where people would like to get a developer job
job_prefs = responses_2017.groupby("JobPref").JobPref.count()
job_prefs.plot.barh()
plt.show() 

# Create a dataframe from the 2017 sheet by providing the sheet's name 
# Create df from second worksheet by referencing its name
responses_2017 = pd.read_excel("fcc_survey.xlsx",
                               sheet_name="2017")

# Graph where people would like to get a developer job
job_prefs = responses_2017.groupby("JobPref").JobPref.count()
job_prefs.plot.barh()
plt.show()

#MULTIPLE SHEET
# load sheets by name in a python list 
# Load all sheets in the Excel file
all_survey_data = pd.read_excel("fcc_survey.xlsx",
                                sheet_name=["2016","2017"])

# View the sheet names in all_survey_data
print(all_survey_data.keys())

# load sheet by position + sheet name
all_survey_data = pd.read_excel("fcc_survey.xlsx",
                                sheet_name=[0,"2017"])

# View the sheet names in all_survey_data
print(all_survey_data.keys())

# To get them all, pass None
# Load all sheets in the Excel file
all_survey_data = pd.read_excel("fcc_survey.xlsx",
                                sheet_name=None)

# View the sheet names in all_survey_data
print(all_survey_data.keys())

# Taking multiple sheets in a workbook and compile into a single dataframe from analysis
# Create an empty dataframe
all_responses = pd.DataFrame()

# Set up for loop to iterate through values in responses
for df in responses.values():
  # Print the number of rows being added
  print("Adding {} rows".format(df.shape[0]))
  # Append df to all_responses, assign result
  all_responses = all_responses.append(df)

# Graph employment statuses in sample
counts = all_responses.groupby("EmploymentStatus").EmploymentStatus.count()
counts.plot.barh()
plt.show()
# ordered dict = responses(keys = sheetnames, dataframes = values)
# values() = method to retrieve the dataframes
"""You compiled similar spreadsheets into one dataset. This method works well when you know 
your spreadsheets use the same column names. If they don't, you can end up with lots of NA values 
where column names don't align"""
# Add this project to github as 
"""
Create an empty dataframe, all_responses.
Set up a for loop to iterate through the values in the responses dictionary.
Append each dataframe to all_responses and reassign the result to the same variable name
"""
# every Datacamp Exercise should be uploaded to your github repos within notebooks
bootcamp_data = pd.read_excel("fcc_survey_booleans.xlsx")
print(bootcamp_data.dtypes)

# Count True Values 
print(bootcamp_data.sum())

# Count NAs
print(bootcamp_data.isna().sum())

# Load data, casting True/False columns as Boolean 
bool_data = pd.read_excel("fcc_survey_booleans.xlsx", 
                          dtype={"AttendedBootcamp": bool,
                                 "AttendedBootcampYesNo": bool,
                                 "AttendedBootcampTF": bool,
                                 "BootcampLoan":  bool,
                                 "LoanYesNo": bool,
                                 "LoanTF": bool})
print(bool_data.dtypes)

# Count True Values 
print(bool_data.sum())

# Count NA values 
print(bool_data.isna().sum())

# Load data with Boolean dtypes and custom T/F values
bool_data = pd.read_excel("fcc_survey_booleans.xlsx", 
                          dtype={"AttendedBootcamp": bool,
                                 "AttendedBootcampYesNo": bool,
                                 "AttendedBootcampTF": bool,
                                 "BootcampLoan":  bool,
                                 "LoanYesNo": bool,
                                 "LoanTF": bool},
                          true_values=["Yes"],
                          false_values=["No"])
print(bool_data.sum())

# README.md
# the following contains a string ID column and several True/False columns indicating financial
# stressors. You'll evaluate which non-ID columns have no NA values and therefore can be set as 
# Boolean, then tell read_excel() to load them as such with the dtype args**

# count the NA values in each column of survey_data with isna fx & sum fx.
# HasDebt has zero NAs as well as ID.x

# Load the data
survey_data = pd.read_excel("fcc_survey_subset.xlsx", 
                        dtype={"AttendedBootcamp": bool,
                                 "AttendedBootcampYesNo": bool,
                                 "AttendedBootcampTF": bool,
                                 "BootcampLoan":  bool,
                                 "LoanYesNo": bool,
                                 "LoanTF": bool}
                                 )

# Count NA values in each column
print(survey_data.isna().sum())

# To view the financial burdens by Boolean group

# Set dtype to load appropriate column(s) as Boolean data
survey_data = pd.read_excel("fcc_survey_subset.xlsx",
                            dtype={
                                "HasDebt": bool,
                            })

# View financial burdens by Boolean group
print(survey_data.groupby("HasDebt").sum())

# Set custom true/false values
# Load file with Yes as a True value and No as a False value
survey_subset = pd.read_excel("fcc_survey_yn_data.xlsx",
                              dtype={"HasDebt": bool,
                              "AttendedBootCampYesNo": bool},
                              true_values=["Yes"],
                              false_values=["No"])

# View the data
print(survey_subset.head())

# you can set the ids of records as the index with Excel 
pd.read_excel(src_file, index_col="id_column")

# to run iteration of columns
for x in df.columns:
  print(x)

#parsing DATES 

# List columns of dates to parse
date_cols = ["Part1StartTime", "Part1EndTime" ]

# Load file, parsing standard datetime column
survey_df = pd.read_excel("fcc_survey.xlsx", parse_dates=date_cols)

# Check data types of timestamp columns 
print(survey_df[["Part1StartTime",
                  "Part1EndTime",
                  "Part2StartDate",
                  "Part2StartTime",
                  "Part2EndTime"]].dtypes)

# Part2StartDate is not in the src data 
# List columns of dates to parse 
date_cols = ["Part1StartTime", "Part1EndTime",[["Part2StartTime","Part2EndTime"]]]

# Load file, parsing standard and split datetime columns 
survey_df = pd.read_excel("fcc_survey.xlsx", parse_dates=date_cols)

print(survey_df.head(3))

# List columns of dates to parse 
date_cols = {"Part1Start": "Part1StartTime",
             "Part1End": "Part1EndTime",
             "Part2StartTime": ["Part2StartDate", 
                                "Part2StartTime"]}

# Load file, parsing standard and split datetime colimns
survey_df = pd.read_excel("fcc_survey.xlsx", parse_dates=date_cols)

print(survey_df.Part2Start.head(3))

# Here's how to format a date
format_string = "%m%d%Y %H:%M:%S"

# formated datetime notation, now pass to the DataFrame
survey_df["Part2EndTime"] = pd.to_datetime(survey_df["Part2EndTime"], format=format_string)

# check the df Series aka Column 
survey_df["Part2EndTime"]

# dbl chck 
print(survey_df.Part2EndTime.head())

# Exercise - parse simple dates 

# Load file, with Part1StartTime parsed as datetime data
date_cols = ["Part1StartTime"]
survey_data = pd.read_excel("fcc_survey.xlsx",
                            parse_dates=date_cols)

# Print first few values of Part1StartTime
print(survey_data.Part1StartTime.head())

# Exercise - get datetimes from multiple columns, split across

# Create dict of columns to combine into new datetime column
datetime_cols = {"Part2Start": ["Part2StartDate", "Part2StartTime"]}


# Load file, supplying the dict to parse_dates
survey_data = pd.read_excel("fcc_survey_dts.xlsx",
                            parse_dates=datetime_cols)

# View summary statistics about Part2Start
print(survey_data.Part2Start.describe())

# things you spend six-figures on like most/least
# least: clothing, makeup, shoes, cars | most: food, travel, otaku, certain clothes, jewelry, handbags, art, nails

# Parse non-standard date formats

# Parse datetimes and assign result back to Part2EndTime
survey_data["Part2EndTime"] = pd.to_datetime(survey_data["Part2EndTime"], 
                                             format="%m%d%Y %H:%M:%S")

# Print first few values of Part2EndTime
print(survey_data.Part2EndTime.head())

# XPATH 
# ("collection/movie[popularity < 8]/format/text()") 
# access the child elements with the XML file 

# to access the other attribute 
#["collection/movie[@title='Transformers']/@shelf"]














