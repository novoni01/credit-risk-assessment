import os
import sqlite3
from pathlib import Path
import pandas as pd
import shutil
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi
import random

def move_kaggle_json():
    """
    Function that automatically moves the 'kaggle.json' file from the user's download folder to the final destination so that all Kaggle API calls work properly.
    """
    #Get the user path 
    user_path = os.path.expanduser("~")
    #Make the path to the kaggle json
    downloads_path = os.path.join(user_path, 'Downloads/kaggle.json')

    #Make the .kaggle folder in user
    kaggle_folder = os.path.join(user_path, ".kaggle")
    os.makedirs(kaggle_folder, exist_ok= True)
    #print(os.path.exists(downloads_path))

    if os.path.exists(downloads_path):
        try:
            print("Moving kaggle.json to user")
            #Move json to the folder
            shutil.move(downloads_path, kaggle_folder)
        except Exception as e:
            print(f"Error when trying to move kaggle.json: {e}")
    else:
        #attempt to check if kaggle.json already exists
        final = os.path.join(kaggle_folder, "kaggle.json")
        if os.path.exists(final):
            print("Kaggle json already exists in user")
        else:
            print("Please download kaggle.json and make sure it is in your downloads folder")

#gather data from a specific csv file and return as a pandas df
def initialize_data_path():
    """
    Returns a path object pointing to the 'training data' folder

    If no folder exists, it will make one
    """
    BASE = Path.cwd()
    DATA = BASE / "training_data"
    #Make sure training_data folder exists
    DATA.mkdir(parents= True, exist_ok= True)
    return DATA


#TO USE THIS API you must have a .kaggle folder in your 'C:\NAME' directory -> then paste the kaggle.json authenticator

def get_kaggle_data(DATA = Path):
    """
    Function that uses Kaggle API to download all datasets hosted by Kaggle

    Params:
        DATA : Path object that points to the 'training data' folder
    """
    try:
        api = KaggleApi()
        api.authenticate()

        #print(api.dataset_list_files('wordsforthewise/lending-club').files)

        api.dataset_download_files('wordsforthewise/lending-club', path= DATA, unzip=True)
    except Exception as e:
        print(f"Error when running get_kaggle_data: {e}")

def retrieve_training_csv(DATA = Path): 
    """ 
    Function that returns all the csv files in the training data folder as a dataframe object 
    
    Params:
        DATA : Path object that points to the 'training data' folder
    """ 
    csv_list = list(DATA.glob("**/*.csv")) 
    return_list = [] 
    #print(csv_list)
    try:
        #traverse each item inthe data path, and if a file ending in .csv is found, turn it into a df and append to return list 
        for item in csv_list: 
            if os.path.isfile(item): 
                print(f"{item} is a file") 
                return_list.append(pd.read_csv(item, low_memory = False)) 
            else: 
                print(f"{item} is folder or dir")

        if return_list: 
            return return_list 
        else: 
            print(f"No csv files found in {DATA}") 
            return []
    except Exception as e:
        print(f"Error when using retrieve_training_csv: {e}")

def get_dir_size(path): 
    """ Get directory size in MBs """ 
    total = 0 
    for dirpath, _, files in os.walk(path):
        for f in files:
            file_path = os.path.join(dirpath, f)
            
            try:
                total += os.path.getsize(file_path) 
                #print(f"{total/1000000} MB")
            except: 
                continue 
        return total/1000000
    
def delete_large_files(DATA = Path): 
    """ 
    Function that deletes the large files downloaded from kaggle to save space. Deletes files to prevent any storage errors when pushing code to github.
    
    Params:
        DATA : Path object that points to the 'training data' folder
    """

    for paths in DATA.glob("**/*"): 
        if os.path.isdir(paths): 
            try:
                size = get_dir_size(paths) 
                print(f"size of {paths} is {size}")

                if size > 100: 
                    print(f"deleting {paths}") 
                    shutil.rmtree(paths) 

            except Exception as e:
                print(f"Error when trying to get size or delete file: {e}")
        else:
            if paths.suffix == ".gz":
                os.remove(paths)
                #print(paths)

def accepted_loans_df(dataframe_list = list, sample_csv = True, seed = 0):
    """
    Returns a cleaned up dataframe of Accepted_Loans with relevant columns for model training
    Params:
        dataframe_list : list containing dataframe objects of all the csv files in the "training_data" folder
        sample_csv (True as default) : True returns a small sample size with a random seed to replicate results. If False is passed the entire cleaned df is returned (2 million entries)
        seed (random seed is default) : integer between 0 and 999, used to replicate random_sample output for debugging. If no seed is passed a random one will be generated
    """
    #Create a random seed for sampling the large dataset if no seed is provided
    if seed == 0:
        seed = random.randint(0,999)
        #print(f"Random seed to replicate accepted loans df: {seed}")
    try:
        raw_accepted_loans = dataframe_list[0].copy()

        #int/float64 columns that will go first in cleaned dataframe
        num_cols = [
            "id", "loan_amnt", "funded_amnt", "term", "int_rate", "installment", "annual_inc",
            "dti", "delinq_2yrs", "fico_range_low", "fico_range_high", "inq_last_6mths",
            "open_acc", "revol_bal", "revol_util", "total_acc", "pub_rec_bankruptcies"
        ]

        #String object columns that will be the last columns of the dataframe
        text_cols = [
            "home_ownership", "loan_status", "purpose", "application_type", "verification_status"
        ]

        #only find columns with valid ids (not string or empty)
        cleaned_accepted_loans = raw_accepted_loans[num_cols + text_cols]
        cleaned_accepted_loans = cleaned_accepted_loans[pd.to_numeric(cleaned_accepted_loans['id'], errors='coerce').notna()]

        #convert id column to int instead of obj
        cleaned_accepted_loans['id'] = cleaned_accepted_loans['id'].astype('int64')

        #fix the the term column to make all the values int (specify in column label that numbers mean months)
        cleaned_accepted_loans.rename(columns = {'term': 'term_months'}, inplace = True)
        num_cols[3] = 'term_months'
        cleaned_accepted_loans['term_months'] = cleaned_accepted_loans['term_months'].str.replace(" months", "")
        cleaned_accepted_loans['term_months'] = cleaned_accepted_loans['term_months'].astype('int64')

        #find any na rows and fill them as needed
        for rows in num_cols:
            cleaned_accepted_loans.loc[:, rows] = cleaned_accepted_loans[rows].fillna(cleaned_accepted_loans[rows].median())

        #Get a sample of the cleaned up df
        if sample_csv:
            sampled_accepted_loans = cleaned_accepted_loans.sample(200000, random_state = seed)
            return sampled_accepted_loans
        else:
            print(f"Returning sample dataframe with random_seed: {sample_csv}")
            print("sample was manually set to false, the entire csv will be returned (large dataframe)")
            return cleaned_accepted_loans
    except Exception as e:
        print("Error in accepted_loans_df: {e}")
        return
    
def rejected_loans_df(dataframe_list = list, sample_csv = True, seed = 0):
    """
    Returns a cleaned up dataframe of Rejected_Loans with relevant columns for model training
    Params:
        dataframe_list : list containing dataframe objects of all the csv files in the "training_data" folder
        sample_csv (True as default) : True returns a small sample size with a random seed to replicate results. If False is passed the entire cleaned df is returned (2 million entries)
        seed (random seed is default) : integer between 0 and 999, used to replicate random_sample output for debugging. If no seed is passed a random one will be generated
    """

    #Create a random seed for sampling the large dataset if no seed is provided
    if seed == 0:
        seed = random.randint(0,999)
        #print(f"Random seed to replicate accepted loans df: {seed}")
    
    try:
        #Create a copy of the rejected loans df
        raw_rejected_loans = dataframe_list[1].copy()
        
        #Drop unneeded columns 
        cleaned_rejected_loans = raw_rejected_loans.drop(columns=['Risk_Score', 'Zip Code', 'State', 'Policy Code', 'Employment Length'])
        rl_cols = list(cleaned_rejected_loans.columns)

        #Fix any missing values in the rejection loans
        for rows in rl_cols:
            #fix the rows that are of float type
            if cleaned_rejected_loans[rows].dtypes == 'float64':
                print(f"replacing {rows} with median float")
                cleaned_rejected_loans.loc[:, rows] = cleaned_rejected_loans[rows].fillna(cleaned_rejected_loans[rows].median())
            else:
                #get the highest repeated string and replace N/A's with that string
                print(f"replacing {rows} na's with most repeated string")
                replacement_string = cleaned_rejected_loans[rows].value_counts().reset_index().at[0,rows]
                cleaned_rejected_loans.loc[:, rows] = cleaned_rejected_loans[rows].fillna(replacement_string)
        #Get a sample of the cleaned up df
        if sample_csv:
            sampled_rejected_loans = cleaned_rejected_loans.sample(200000, random_state = seed)
            return sampled_rejected_loans
        else:
            print(f"Returning sample dataframe with random_seed: {sample_csv}")
            print("sample was manually set to false, the entire csv will be returned (warning: large dataframe)")
            return cleaned_rejected_loans
    except Exception as e:
        print("Error in rejected_loans_df: {e}")
        return