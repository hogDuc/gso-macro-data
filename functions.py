from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
import os
import time
import pickle
import pandas as pd
import re
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service as ChromeService

from functions import *
from typing import Literal
pd.options.mode.copy_on_write = True

# Disable verification warning when accessing GSO site
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_data(download_path, url_list):

    '''
    Download data from the given list of URLS
    Args:
        download_path: folder to save the files
        url_list: List of urls to download
    Output:
        Error urls
    '''
    
    # Set Chrome Driver Options
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory" : download_path,
        "download.prompt_for_download": False
    }
    options.add_experimental_option("prefs",prefs)
    options.binary_location = "./chrome-win64/chrome.exe"
    options.add_argument("--headless")

    error_url = []
    for url in tqdm(url_list): # Change to all_reports_url to crawl all data
        try: 
            service = ChromeService(executable_path = "./chromedriver-win64/chromedriver.exe")
            driver = webdriver.Chrome(service = service, options=options)
            driver.get(url)

            wait = WebDriverWait(driver, 10)
            download_link = wait.until(EC.element_to_be_clickable((By.XPATH, '//a[contains(@href, ".xlsx") or contains(@href, ".xls)]')))
            download_link.click()

            # Wait for files to finish downloading
            download_complete = False
            while not download_complete:
                files = os.listdir(download_path)
                if any(file.endswith('.crdownload') for file in files) or any(file.endswith(".tmp") for file in files):
                    time.sleep(1)
                else:
                    download_complete = True
        except Exception as error:
            print(f"Error at: {url}")
            error_url.append(url)
        driver.quit()
    return error_url

def add_month(df, excel_path):
    '''
    Converting excel file name to datetime
    '''

    pattern = re.compile(r"(\d{1,2})[-.](20\d{2})")
    match = pattern.search(excel_path)
    month, year = match.groups()

    df = df.assign(
        month = pd.Timestamp(
            datetime(year=int(year), month=int(month), day=1)
        ) + pd.DateOffset(months=1)
    )
    return df

def use_columns(excel_path="", sheet_index=0, col_index=None):
    '''
    Params:
        excel_path: Excel file name, default with search in folder 'raw_xlsx'
        sheet_index: numeric index of sheet, starts with 0
        col_index: index of columns to keep (default: all columns)
    Output:
        Input dataframe with date column
    '''
    excel_file = os.path.join("raw_xlsx", excel_path)
    sheetnames = pd.ExcelFile(excel_file).sheet_names[sheet_index]
    sheet = pd.read_excel(excel_file, sheet_name=sheetnames, header=None)
    if col_index == None:
        return add_month(sheet.dropna(how='all'), excel_path)
    else:
        sheet = sheet.iloc[:, col_index].dropna(how='all')
    return add_month(sheet, excel_path)

# Filter out data lines with text
def filter_numeric_rows(df, columns):
    """
    Filter rows where at least one of the specified columns contains a numeric value.
    columns (list): List of column names to check for numeric values.
    """
    # mask = False
    # for col in columns:
    #     mask = mask | pd.to_numeric(df[col], errors="coerce").notnull()
    # return df[mask].reset_index(drop=True)
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def clean_data(df, key_column, columns):
    return(
        filter_numeric_rows(df.dropna(subset=key_column), columns)
    )

def crawl_url():
    all_reports_url = []
    for page in tqdm(range(1, 28), desc="Scraping page"):
        url = f'https://www.nso.gov.vn/en/monthly-report/?paged={page}'
        req = requests.get(url=url, verify=False)
        page = BeautifulSoup(req.content, "html.parser")
        url_list = [
            link.get("href") for link in page.find("div", class_="archive-container").find_all("a", attrs={"class":None})
        ]
        all_reports_url.extend(url_list)
    with open("all_reports_url.pkl", "wb") as f:
        pickle.dump(all_reports_url, f)
    return all_reports_url

# Check if data have the same sheet names
# Quarterly data and January data files are formatted differently, so they will be separated into a different dataset
def check_columns(name_list: list[Literal["quarterly_files", "monthly_files", "january_files"]]):
    '''
    Args:
        name_list: 
    '''
    sheet_names = []
    for file in name_list:
        excel = os.path.join("raw_xlsx", file)
        try:
            sheet_names.append(pd.ExcelFile(excel).sheet_names)
        except:
            print(f"Failed to read file: {file}")
    data = pd.DataFrame(sheet_names).transpose()
    try:
        data.columns = name_list
    except:
        print(f"Failed to change column names of file: {file}")
    return data

def combine_columns(df: pd.DataFrame, n_columns: int):

    '''
    Combine first n_columns to create usable item names
    Args:
        df: Input dataframe
        n_columns: Number of columns to merge into one
        
    Output:
        Dataframe with a new "name" column
    '''
    df = df.rename(columns={
        0:"name"
    })
    for i in range(0, n_columns - 1):
        df = df.assign(
            name = lambda df : df.loc[:, "name"].combine_first(df[i+1])
        )
    return df

def remove_duplicates(folder_path: str):
    '''
    Delete duplicated files from folder
    Args:
        folder_path: Path to the folder that you would like to remove duplicates
    '''

    all_files = os.listdir(folder_path)
    duplicate_pattern = re.compile(r'^(.*)\s\(\d+\)(\.\w+)$')

    original_files = set()

    for file in all_files:
        match = duplicate_pattern.match(file)
        if match:
            base_name = match.group(1) + match.group(2)
            original_files.add(base_name)

    # Now delete duplicates
    for file in all_files:
        if duplicate_pattern.match(file):
            file_path = os.path.join(folder_path, file)
            try:
                os.remove(file_path)
                print(f"Deleted: {file}")
            except Exception as e:
                print(f"Failed to delete {file}: {e}")
