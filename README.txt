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

data_ingestion_kaggle.py (Specific functions for Kaggle data)
    Module containing finalized functions to bring our project to life. Each project has a short description with how it works and the parameters required

data_ingestion_hdma.py (Specific functions for HDMA data)
    Module containing finalized functions to bring our project to life. Each project has a short description with how it works and the parameters required. 