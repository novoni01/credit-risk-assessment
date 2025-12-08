Sources used:
    Kaggle
        - https://www.kaggle.com/datasets/wordsforthewise/lending-club
    Datacamp
        - https://www.datacamp.com/datalab/datasets/dataset-python-loans
    HMDA (Home Mortgage Disclosure Act)
        - https://ffiec.cfpb.gov/data-publication/snapshot-national-loan-level-dataset/2024

TO USE KAGGLE API (Manually)
In order to use Kaggle api you need to 
    1. Create a free kaggle account
    2. Click on your profile picture
    3. Download "Legacy API Key" that should produce a 'kaggle.json' file in your download folder
    4. Call the move_kaggle_json() function

CRA.ipynb
    The notebook is used to sketch out ideas, see what works and what doesn't. All finalized code will be put in the cra_functions.py module for easy
    imports. This way all the code can be imported into other files and ran with ease. 

data_ingestion_kaggle.py
    Module containing finalized functions to bring our project to life. Each project has a short description with how it works and the parameters required

TO SETUP DATABASE (locally)
    In your terminal root folder:
        1. psql -d postgres
            (in psql) CREATE DATABASE credit_risk;
            (in psql) \l -- to verify u created it
            (in psql) \q -- to exit
        2. psql -d credit_risk -f database/database.sql
        3. psql -d credit_risk -f database/staging.sql
            sanity check: 
                psql -d credit_risk
                (in psql) \dt -- should show the tables have all been loaded (total 7)
                (in psql) \q
        4. populate staging tables 
            python -m ETL.transformation.staging_loader -- run ONCE!!!
            sanity check:
                psql -d credit_risk
                -- check that the count of rows for each of the staging table is 200000, if its more then you loaded twice by accident and need to delete tables and redo
                (in psql)
                    SELECT 'staging_accepted_hdma' AS table, COUNT(*) FROM staging_accepted_hdma;
                    SELECT 'staging_accepted_kaggle', COUNT(*) FROM staging_accepted_kaggle;
                    SELECT 'staging_rejected_hdma', COUNT(*) FROM staging_rejected_hdma;
                    SELECT 'staging_rejected_kaggle', COUNT(*) FROM staging_rejected_kaggle;
        5. mapping and loading into normalized schema
            python -m ETL.transformation.transf_loader -- run ONCE!!
            sanity check:
                psql -d credit_risk
                (in psql)
                    SELECT COUNT(*) FROM borrowers; -- should be 266723
                    SELECT COUNT(*) FROM accepted_loans; -- should be 400000
                    SELECT COUNT(*) FROM rejected; -- should be 400000


