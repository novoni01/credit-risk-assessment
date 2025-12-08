#populate staging tables

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import pandas as pd
from typing import Optional, Dict

from ETL.ingestion.data_ingestion_kaggle import (
    initialize_data_path as kaggle_data_path,
    retrieve_training_csv,
    kaggle_accepted_loans_df,
    kaggle_rejected_loans_df,
)
from ETL.ingestion.data_ingestion_hdma import (
    initialize_data_path as hdma_data_path,
    clean_hdma_accepted,
    clean_hdma_rejected,
)

#helpers -----------
#ret num of rows
def write_df_to_table(engine: Engine, df: pd.DataFrame, table_name: str, if_exists: str = "append", chunksize: int = 5000,) -> int:
    if df is None or df.empty:
        print(f"[staging_loader] for {table_name} is empty")
        return 0

    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False, method="multi", chunksize=chunksize,)

    return len(df)

# load staging -------------
def load_kaggle_staging(sample: bool = True, seed: int = 0, engine: Optional[Engine] = None,) -> None:
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    kaggle_csvs = retrieve_training_csv(kaggle_data_path())

    accepted_df = kaggle_accepted_loans_df(kaggle_csvs, sample_csv=sample, seed=seed)
    rejected_df = kaggle_rejected_loans_df(kaggle_csvs, sample_csv=sample, seed=seed)

    print(f"[staging_loader] Kaggle accepted rows: {len(accepted_df)}")
    print(f"[staging_loader] Kaggle rejected rows: {len(rejected_df)}")

    accepted_cols = ["id","loan_amnt","funded_amnt","term_months","int_rate","installment","annual_inc","dti","delinq_2yrs","fico_range_low","fico_range_high","inq_last_6mths","open_acc","revol_bal","revol_util","total_acc","pub_rec_bankruptcies","home_ownership","loan_status","purpose","application_type","verification_status"]
    rejected_cols = ["Amount Requested","Application Date","Loan Title","Debt-To-Income Ratio",]

    accepted_stage = accepted_df[accepted_cols].copy()
    inserted_accepted = write_df_to_table(engine, accepted_stage, "staging_accepted_kaggle", "append", 5000)
    print(f"[staging_loader] inserted {inserted_accepted} rows inside staging_accepted_kaggle")

    rejected_stage = rejected_df[rejected_cols].copy()
    inserted_rejected = write_df_to_table(engine,rejected_stage,table_name="staging_rejected_kaggle",if_exists="append",chunksize=5000,)
    print(f"[staging_loader] inserted {inserted_rejected} rows inside staging_rejected_kaggle")

def load_hdma_staging(sample: bool = True,seed: int = 0,engine: Optional[Engine] = None,) -> None:
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    accepted_df = clean_hdma_accepted(hdma_data_path(),sample=sample,seed=seed,)
    rejected_df = clean_hdma_rejected(hdma_data_path(),sample=sample,seed=seed,)

    print(f"[staging_loader] HDMA accepted rows: {len(accepted_df)}")
    print(f"[staging_loader] HDMA rejected rows: {len(rejected_df)}")

    accepted_cols = ['activity_year', 'action_taken', 'preapproval', 'loan_purpose', 'loan_amount', 'loan_term', 'applicant_credit_score_type',
        'co_applicant_credit_score_type' , 'loan_to_value_ratio', 'income', 'debt_to_income_ratio',
        'derived_loan_product_type',]
    accepted_stage = accepted_df[accepted_cols].copy()
    inserted_acc = write_df_to_table(engine,accepted_stage,table_name="staging_accepted_hdma",if_exists="append",chunksize=5000,)
    print(f"[staging_loader] Inserted {inserted_acc} rows inside staging_accepted_hdma")

    rejected_cols = ["activity_year","action_taken","preapproval","loan_purpose","loan_amount","loan_term","loan_to_value_ratio","income","debt_to_income_ratio","derived_loan_product_type","applicant_credit_score_type","co_applicant_credit_score_type","denial_reason_1",]
    rejected_stage = rejected_df[rejected_cols].copy()

    # print(rejected_stage.dtypes)
    # print(rejected_stage[['loan_amount',loan_term','loan_to_value_ratio','income','debt_to_income_ratio']].describe())

    inserted_rejec = write_df_to_table(engine,rejected_stage,table_name="staging_rejected_hdma",if_exists="append",chunksize=5000,)
    print(f"[staging_loader] Inserted {inserted_rejec} rows inside staging_rejected_hdma")

if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2:///credit_risk")

    print("=== Loading Kaggle staging tables ===")
    load_kaggle_staging(sample=True, seed=42, engine=engine)

    print("=== Loading HDMA staging tables ===")
    load_hdma_staging(sample=True, seed=42, engine=engine)

    print("=== All staging tables populated ===")