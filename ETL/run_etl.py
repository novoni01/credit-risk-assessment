from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from ETL.transformation.staging_loader import (
    load_kaggle_staging,
    load_hdma_staging
)

from ETL.transformation.validation_loader import (
    create_valid_accepted_kaggle,
    create_valid_rejected_kaggle,
    create_valid_accepted_hdma,
    create_valid_rejected_hdma,
    confirm_lengths
)

from ETL.load.transf_loader import (
    run_transf_loader
)

from ETL.ingestion.data_ingestion_kaggle import (
    initialize_data_path,
    delete_large_files
)

from ETL.transformation.ml_training_acc import (
    create_accepted_loans_training_view,
    preview_training_counts
)

def create_databases() -> None:
    sql_files = [
        "database/database.sql",
        "database/indexing.sql",
        "database/staging.sql"
    ]

    engine = create_engine("postgresql+psycopg2:///credit_risk")
    
    with engine.begin() as conn:
        for path in sql_files:
            print(f"=== Creating SQL file: {path} ===")
            with open(path, 'r') as f:
                sql = f.read()
            conn.execute(text(sql))

    print("=== Completed database creation ===")

if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2:///credit_risk")
    try:
        create_databases()
    except Exception as e:
        print(f"Could not create database: {e}")

    try:
        print("=== Loading Kaggle staging tables ===")
        load_kaggle_staging(sample=True, seed=42, engine=engine)

        print("=== Loading HDMA staging tables ===")
        load_hdma_staging(sample=True, seed=42, engine=engine)

        print("=== All staging tables populated ===")
    except Exception as e:
        print(f"Error when trying to execute staging_loader.py: {e}")

    try:
        print("=== Loading VALID Kaggle accepted table ===")
        create_valid_accepted_kaggle(engine)

        print("=== Loading VALID Kaggle rejected table ===")
        create_valid_rejected_kaggle(engine)

        print("=== Loading VALID HDMA accepted table ===")
        create_valid_accepted_hdma(engine)

        print("=== Loading VALID HDMA rejected table ===")
        create_valid_rejected_hdma(engine)

        print("=== ALL VALID TABLES WERE LOADED ===")
        confirm_lengths(engine)

    except Exception as e:
        print(f"Error when trying to execute validation_loader.py: {e}")

    try:
        run_transf_loader(engine)
    except Exception as e:
        print(f"Error when trying to execute transf_loader.py: {e}")

    try:
        path = initialize_data_path()
        delete_large_files(path)
        print("=== DELETED all large files (KAGGLE!) ===")
    except Exception as e:
        print(f"Error when trying to delete large files: {e}")
    
    try:
        print("=== Creating ML TRAINING VIEW ===")
        create_accepted_loans_training_view(engine)
        preview_training_counts(engine)
    except Exception as e:
        print(f"Error when trying to execute ml_training_acc: {e}")