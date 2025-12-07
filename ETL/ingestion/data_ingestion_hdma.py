import os
from pathlib import Path
import pandas as pd
import shutil
import csv
import kaggle
import random

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

def clean_data(df = pd.DataFrame(), sample = bool, seed = int): 
    """
    Helper function made to clean the HDMA dataframes
    """

    int_values = [
        'activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_term', 'applicant_credit_score_type',
        'co-applicant_credit_score_type' , 'denial_reason-1'
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
    
    #Fix the 1111's in denial reason
    df['denial_reason-1'] = df['denial_reason-1'].replace(1111, 1)

    #Fix any exempts
    exempt_cols = ['debt_to_income_ratio', 'income', 'loan_term', 'loan_to_value_ratio']

    for col in exempt_cols:
        df[col] = df[col].replace('Exempt', df[col].value_counts().reset_index().iat[0, 0])

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

    try:
        if sample is True and seed != 0:
                #Create a random seed for sampling the large dataset if no seed is provided
                return df_no_outliers.sample(200000, random_state = seed)
        elif sample is True:
                seed = random.randint(0,999)
                print(f"Random seed to replicate accepted loans df: {seed}")
                return df_no_outliers.sample(200000, random_state = seed)
        else:
            print(f"Sample input was set --> {sample}, thus the entire dataset will be returned")
            return df_no_outliers
    except Exception as e:
        print(f"Error when attempting to get the dataframe cleaned up : {e}")
        return

def read_hdma(file = Path):
    """
    Helper function that retrieves specific data from the gzip files
    Params:
        DATA : Path object that points to the 'training data' folder
    """
    filtered_columns = [
        'activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_term', 'applicant_credit_score_type',
        'co-applicant_credit_score_type' , 'denial_reason-1', 'loan_to_value_ratio', 'income', 'debt_to_income_ratio',
        'derived_loan_product_type' 
    ]

    try:
        df_recovered = pd.read_parquet(file, columns=filtered_columns)
        return df_recovered
    except:
        print(f"Error when attempting to retrive {file} using specific columns function")

def clean_hdma_rejected(DATA = Path, sample = True, seed = 0):
    """
    Function that reads the parquet.gzip file to return a cleaned dataframe with only rejected loans in the US during 2023
    https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields#loan_amount
    The link above provides details in regards to each column
    Params:
        DATA : Path object that points to the 'training data' folder
        seed (random seed is default) : integer between 0 and 999, used to replicate pd.random_sample output for debugging. If no seed is passed a random one will be generated
    """
    try:
        hdma_rejected_parquet = DATA / 'hdma_rejected_raw.parquet.gzip'
        
        rejected_df = read_hdma(hdma_rejected_parquet)
        rejected_cleaned = clean_data(rejected_df, sample, seed)
        return rejected_cleaned
    except Exception as e:
        print(f"Error when retrieving rejected HDMA as a df: {e}")

def clean_hdma_accepted(DATA = Path, sample = True, seed = 0):
    """
    Function that reads the parquet.gzip file to return a cleaned dataframe with only rejected loans in the US during 2023
    https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields#loan_amount
    The link above provides details in regards to each column
    Params:
        DATA : Path object that points to the 'training data' folder
        seed (random seed is default) : integer between 0 and 999, used to replicate pd.random_sample output for debugging. If no seed is passed a random one will be generated
    """
    try:
        hdma_accepted_parquet = DATA / 'hdma_accepted_raw.parquet.gzip'
        accepted_df = read_hdma(hdma_accepted_parquet)
        accepted_cleaned = clean_data(accepted_df, sample, seed)
        return accepted_cleaned.drop(columns=['denial_reason-1'])
    except Exception as e:
        print(f"Error when retrieving accepted HDMA as a df: {e}")