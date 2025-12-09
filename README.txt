Sources used:
    Kaggle
        - https://www.kaggle.com/datasets/wordsforthewise/lending-club
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
--NOTE: if you get a login error that looks like " fatal error provided for your_computer_user" and you can't login
    -> Open terminal on windows
    -> C:\Users\your_name
    -> setx PGUSER postgres (copy/type this)
    -> then close & reopen your text editor, try again
--NOTE: if you want to skip entering the password everytime
    -> open your file explorer
    -> find your OS(C:) and click it 
    -> go to 'Program Files' then 'PostgreSQL' then the version you installed, latest version is 18 (or go to wherever you installed your PostgreSQL directory files)
    -> go to 'data' folder then 'pg_hba.conf'
    -> open with a text editor and change all the METHOD values to 'trust'
        -> this will auto trust everyone logging in to your database to make changes
        -> only do this if you only use locally hosted databases
    In your terminal root folder:
        1. psql -d postgres
            (in psql) CREATE DATABASE credit_risk;
            (in psql) \l -- to verify u created it
            (in psql) \q -- to exit
        2. execute the entire etl process in one line of code
            python -m ETL.run_etl -- run ONCE!! (please be patient, takes a bit of time but its the only command you have to run)

        READ!!! the below has been automated in  'run_etl.py', however the individual steps are being left so that anyone can debug if something doesn't work
        1a. psql -d credit_risk -f database/database.sql
        1b. psql -d credit_risk -f database/indexing.sql
        1c. psql -d credit_risk -f database/staging.sql
            sanity check: 
                psql -d credit_risk
                (in psql) \dt -- should show the tables have all been loaded (total 7)
                (in psql) \q

        
        2. populate staging tables 
            python -m ETL.transformation.staging_loader -- run ONCE!!! (slow process, let it load)
            sanity check:
                psql -d credit_risk
                -- check that the count of rows for each of the staging table is 200000, if its more then you loaded twice by accident and need to delete tables and redo
                (in psql)
                    SELECT 'staging_accepted_hdma' AS table, COUNT(*) FROM staging_accepted_hdma;
                    SELECT 'staging_accepted_kaggle', COUNT(*) FROM staging_accepted_kaggle;
                    SELECT 'staging_rejected_hdma', COUNT(*) FROM staging_rejected_hdma;
                    SELECT 'staging_rejected_kaggle', COUNT(*) FROM staging_rejected_kaggle;
        3. validate all data from staging tables
            python -m ETL.transformation.validation_loader -- run ONCE!!!
            -- depending on the random_seed used, the following values will always change. However with the same random seed the counts should be consistent
            sanity check (script should auto pull length of datasets):
                psql -d credit_risk
                    SELECT 'valid_accepted_hdma' AS table, COUNT(*) FROM valid_accepted_hdma; (199803)
                    SELECT 'valid_accepted_kaggle', COUNT(*) FROM valid_accepted_kaggle; (82074)
                    SELECT 'valid_rejected_hdma', COUNT(*) FROM valid_rejected_hdma; (186596)
                    SELECT 'valid_rejected_kaggle', COUNT(*) FROM valid_rejected_kaggle; (138132)
        4. mapping and loading into normalized schema
            python -m ETL.load.transf_loader -- run ONCE!! (slow process, let it load)
            sanity check:
                psql -d credit_risk
                (in psql)
                    SELECT COUNT(*) FROM borrowers; -- should be 134647
                    SELECT COUNT(*) FROM accepted_loans; -- should be 281877
                    SELECT COUNT(*) FROM rejected; -- should be 324728


