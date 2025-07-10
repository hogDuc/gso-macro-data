import json
import re
from functions import *

# Disable verification warning when accessing GSO site
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# PARAMETERS
### Set to True if new reports are updated
get_report_url = False
### Set to True to check if the values are appropriate, False to export them as csv, and combine them into one Excel file
check_values = True
number_of_sheets = 19 # NOTE: Update to be dynamic
### Download data?
download = False
### Test run?
test_run = True
n_files = 24 # Number of files to download,
### .xlsx download path
download_path = os.path.join(os.getcwd(), "raw_xlsx")
### Column names for each monthly sheets
with open("sheetnames.json") as js:
    sheet_cols = json.load(js)


# Get new report url NOTE: update function to detect which report is already crawled
if get_report_url == True:
    all_reports_url = crawl_url()
else:
    with open("all_reports_url.pkl", "rb") as f:
        all_reports_url = pickle.load(f)

if download == True:
    if test_run == True:
        url_list = all_reports_url[0:n_files+1]
    else:
        url_list = all_reports_url
    download_data(download_path, url_list)

'''
There are 3 types of dataformats:
- Quarterly reports: include data of Mar, Jun, Sep, Dec
- January reports
- Other months
'''
quarterly_files = []
monthly_files = []
january_files = []

for excel_path in os.listdir(download_path):
    if any(q in excel_path for q in ["Q", "03", "06", "09", "12", "T3", "T6", "T9", "T12"]):
        quarterly_files.append(excel_path)
    elif any(m in excel_path for m in ["01", "T01"]):
        january_files.append(excel_path)
    else:
        monthly_files.append(excel_path)   

# print(quarterly_files)
# print(january_files)

# Process monthly data
for sheet_index in tqdm(range(0, number_of_sheets)):
    try:
        combine_df = pd.DataFrame()

        if sheet_index == 15:
            row_adj = 1
        else:
            row_adj = 0
        for excel_path in monthly_files:
            sheet = use_columns(excel_path, sheet_index).iloc[1 + row_adj:,:]
            combine_df = pd.concat([combine_df, sheet], axis = 0)

        # Custom conditions
        if sheet_index in [0, 10, 11, 13, 14, 15, 16, 17, 18]: # Sheets that need to merge multiple name columns to make sense
            if sheet_index == 15:
                ncol = 3
                bad_label = combine_df.iloc[:,1].astype(str).str.contains(r"Of which:", regex = True, flags = re.IGNORECASE)
                combine_df.loc[bad_label, 1] = None 
            else:
                ncol = 2
            if sheet_index in [16, 17]:
                is_parent = combine_df.iloc[:,0].str.startswith("By ", na=False)
                combine_df["name"] = (
                    (combine_df.iloc[:,0].where(is_parent).ffill()).astype(str) + 
                    " - " + 
                    combine_df.iloc[:,0].astype(str)
                ).str.replace(r"nan", "", regex=True).replace(r"^\s-\s", "", regex=True).replace("", None)
            else:
                combine_df = combine_columns(combine_df, ncol)
            var_list = ["month", "name"]
        else:
            var_list = ["month"]
        if sheet_index == 2:
            adjust = 1
        else:
            adjust = 0

        var_list.extend(list(sheet_cols["monthly"][f"{sheet_index}"]["columns"].values()))

        combine_df = combine_df.rename(columns=dict(
            list(zip(
                list(map(int, sheet_cols["monthly"][f"{sheet_index}"]["columns"].keys())),
                list(sheet_cols["monthly"][f"{sheet_index}"]["columns"].values())
            ))
        ))[var_list] # Rearranging the columns

        if check_values == False:
            combine_df = clean_data(
                combine_df,
                var_list[1],
                combine_df.columns[2 + adjust:]
            )
            combine_df.to_csv(f"combined_data/{sheet_cols["monthly"][f"{sheet_index}"]["sheet"]}.csv", index=False)
    except Exception as error:
        print(f"Error at {sheet_index}: {error}")