import os
import sqlite3
from pathlib import Path
import pandas as pd
import shutil
import csv
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

def get_kaggle_data(DATA = Path):
    """
    Function that uses Kaggle API to download all datasets hosted by Kaggle

    Params:
        DATA : Path object that points to the 'training data' folder

    Returns:
        Nothing
    """
    try:
        api = KaggleApi()
        api.authenticate()

        #print(api.dataset_list_files('wordsforthewise/lending-club').files)

        api.dataset_download_files('wordsforthewise/lending-club', path= DATA, unzip=True)
    except Exception as e:
        print(f"Error when running get_kaggle_data: {e}")

#TO USE THIS API you must have a .kaggle folder in your 'C:\NAME' directory -> then paste the kaggle.json authenticator
def retrieve_training_csv(DATA = Path): 
    """ 
    Function that returns all the csv files in the training data folder as a dataframe object 
    
    Params:
        DATA : Path object that points to the 'training data' folder
    
    Returns:
        List : dataframe objects list of the kaggle csvs
        Index 0 : Accepted Loans Df
        Index 1 : Rejected Loans Df
    """ 
    #Get kaggle data into your directory
    get_kaggle_data(DATA)
    
    csv_list = list(DATA.glob("**/*.csv")) 
    return_list = [] 
    #print(csv_list)
    try:
        #traverse each item in the data path, and if a file ending in .csv is found, turn it into a df and append to return list 
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

def kaggle_accepted_loans_df(dataframe_list = list, sample_csv = True, seed = 0):
    """
    Returns a cleaned up dataframe of Kaggle -> Accepted_Loans with relevant columns for model training
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
        print(f"Error in kaggle_accepted_loans_df: {e}")
        return
    
def kaggle_rejected_loans_df(dataframe_list = list, sample_csv = True, seed = 0):
    """
    Returns a cleaned up dataframe of Kaggle -> Rejected_Loans with relevant columns for model training
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

def raw_hdma_accepted_df(DATA = Path):
    """
    Function that reads the parquet.gzip file to return a the raw accepted gzip as a dataframe (WARNING LARGE FILE)

    Params:
        DATA : Path object that points to the 'training data' folder
    """
    
def hdma_accepted_df(DATA = Path):
    """
    Function that reads the parquet.gzip file to return a cleaned dataframe with only accepted loans in the US during 2023

    Params:
        DATA : Path object that points to the 'training data' folder
    """
    try:
        hdma_accepted_parquet = DATA / 'hdma_accepted_raw.parquet.gzip'
        temp_num_cols = [
        'activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_to_value_ratio',
        'interest_rate', 'total_loan_costs', 'loan_term', 'income', 'debt_to_income_ratio'
        ]
        temp_text_cols = [
            'derived_loan_product_type', 'applicant_credit_score_type', 'co-applicant_credit_score_type',
            'denial_reason-1', 'denial_reason-2', 'denial_reason-3', 'denial_reason-4'
            ]
        accepted_recovered = pd.read_parquet(hdma_accepted_parquet, columns=temp_num_cols + temp_text_cols)
        return accepted_recovered
    except Exception as e:
        print(f"Error when retrieving hdma_accepted_df: {e}")


def clean_data(df = pd.DataFrame()): 
    """
    Helper function made to clean the HDMA dataframes
    """

    int_values = [
        'activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_term', 'applicant_credit_score_type',
        'co-applicant_credit_score_type', 'denial_reason-1'
    ]

    float_values = [
        'loan_to_value_ratio', 'income', 'debt_to_income_ratio'
    ]

    string_values = [
        'derived_loan_product_type'
    ]
    #Convert any ranges in the debt_income cat into the middle of that range, or leave it at that range if its too broad
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace(">60%", 60)
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace("50%-60%", 55)
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace("20%-<30%", (29+20)/2)
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace("30%-<36%", (35+30)/2)
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace("<20%", 20)
    
    #Fix any exempts
    df['debt_to_income_ratio'] = df['debt_to_income_ratio'].replace('Exempt', df['debt_to_income_ratio'].value_counts().reset_index().iat[0, 0])

    #Fix the income values
    df['income'] = df['income'].replace('Exempt', df['income'].value_counts().reset_index().iat[0, 0])

    #Fix the loan term
    df['loan_term'] = df['loan_term'].replace('Exempt', df['loan_term'].value_counts().reset_index().iat[0, 0])
    
    #fix any rows that have exempt
    df['loan_to_value_ratio'] = df['loan_to_value_ratio'].replace('Exempt', df['loan_to_value_ratio'].value_counts().reset_index().iat[0, 0])

    #Convert all columns to numerical types
    for column in int_values:
        df[column] = df[column].fillna(df[column].value_counts().reset_index().iat[0, 0])
        try:
            df[column] = df[column].astype('int32')
        except Exception as e:
            print(f"skipping int conversion: {column} due to error: {e}") 
            continue  
        

    for column in float_values:
        df[column] = df[column].fillna(df[column].value_counts().reset_index().iat[0, 0])   
        try:
            df[column] = df[column].astype('float64')
        except Exception as e:
            print(f"skipping float conversion: {column} due to error: {e}") 
            continue

    for column in string_values:
        df[column] = df[column].fillna(df[column].value_counts().reset_index().iat[0, 0])

    #Remove outliers based on z score
    loan_std = df['loan_amount'].std()
    loan_mean = df['loan_amount'].mean()
    temp_z_data = df.copy()
    temp_z_data['z_loan'] = ((df['loan_amount'] - loan_mean) / loan_std)
    threshold = 3

    #remove based on threshold
    df_no_outliers = temp_z_data[(temp_z_data['z_loan'].abs()) <= threshold].drop(columns=['z_loan'])
    return df_no_outliers

def read_hdma(file = Path):
    """
    Helper function that retrieves specific data from the gzip files
    Params:
        DATA : Path object that points to the 'training data' folder
    """
    temp_num_cols = [
        'activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_term', 'loan_to_value_ratio',
        'income', 'debt_to_income_ratio'
        ]
    temp_text_cols = [
        'derived_loan_product_type', 'applicant_credit_score_type', 'co-applicant_credit_score_type', 'denial_reason-1'
    ]
    try:
        df_recovered = pd.read_parquet(file, columns=temp_num_cols + temp_text_cols)
        return df_recovered
    except:
        print(f"Error when attempting to retrive {file} using specific columns function")

def clean_hdma_rejected(DATA = Path):
    """
    Function that reads the parquet.gzip file to return a cleaned dataframe with only rejected loans in the US during 2023
    https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields#loan_amount
    The link above provides details in regards to each column
    Params:
        DATA : Path object that points to the 'training data' folder
    """
    try:
        hdma_rejected_parquet = DATA / 'hdma_rejected_raw.parquet.gzip'
        
        rejected_df = read_hdma(hdma_rejected_parquet)
        rejected_cleaned = clean_data(rejected_df)
        return rejected_cleaned
    except Exception as e:
        print(f"Error when retrieving rejected HDMA as a df: {e}")

def clean_hdma_accepted(DATA = Path):
    """
    Function that reads the parquet.gzip file to return a cleaned dataframe with only rejected loans in the US during 2023
    https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields#loan_amount
    The link above provides details in regards to each column
    Params:
        DATA : Path object that points to the 'training data' folder
    """
    try:
        hdma_accepted_parquet = DATA / 'hdma_accepted_raw.parquet.gzip'
        accepted_df = read_hdma(hdma_accepted_parquet)
        accepted_cleaned = clean_data(accepted_df)
        return accepted_cleaned
    except Exception as e:
        print(f"Error when retrieving accepted HDMA as a df: {e}")