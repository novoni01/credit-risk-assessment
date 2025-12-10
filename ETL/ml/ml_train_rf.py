# predict loan default using training view
# python -m ETL.ml.ml_train_rf

"""
output:

Loaded 81460 rows and 35 columns from accepted_loans_ml_training
Label distribution (is_default):
is_default
0    71365 (good)
1    10095 (bad)
Name: count, dtype: int64 

Numeric features: ['loan_amnt', 'funded_amnt', 'term_months', 'int_rate', 'installment', 'annual_inc', 'dti', 'delinq_2yrs', 'fico_range_low', 'fico_range_high', 'inq_last_6mths', 'open_acc', 'total_acc', 'revol_bal', 'revol_util', 'pub_rec_bankruptcies']
Categorical features: ['purpose', 'application_type', 'activity_year', 'action_taken', 'preapproval', 'loan_to_value_ratio', 'total_loan_costs', 'derived_loan_product_type', 'loan_purpose', 'home_ownership', 'verification_status', 'income', 'debt_to_income_ratio', 'applicant_credit_score_type', 'co_applicant_credit_score_type']
Train size: (65168, 31)
Test size: (16292, 31)

=== Training model ===
Training complete.

=== Evaluation on TEST set ===

Classification report:
              precision    recall  f1-score   support

           0       0.88      1.00      0.93     14273
           1       0.57      0.00      0.00      2019

    accuracy                           0.88     16292
   macro avg       0.72      0.50      0.47     16292
weighted avg       0.84      0.88      0.82     16292

Confusion matrix (rows=true, cols=pred):
[[14270     3]
 [ 2015     4]]

Saved model to: /Users/novonibanerjee/Documents/GitHub/credit-risk-assessment/artifacts/accepted_default_rf.joblib
"""

import numpy as np
import pandas as pd

from pathlib import Path
from sqlalchemy import create_engine

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix

import joblib

DB_URL = "postgresql+psycopg2:///credit_risk"
NAME = "accepted_loans_ml_training"
MOD_PATH = Path("artifacts") / "accepted_default_rf.joblib"

def load_training_data(db_url: str = DB_URL) -> pd.DataFrame:
    engine = create_engine(db_url)
    
    df = pd.read_sql(f"SELECT * FROM {NAME}", engine)

    print(f"Loaded {df.shape[0]} rows and {df.shape[1]} columns from {NAME}")
    print("Label distribution (is_default):")
    print(df["is_default"].value_counts(), "\n")

    return df

#preprocessing pipline
def build_pipeline(X: pd.DataFrame) -> tuple[Pipeline, list[str], list[str]]:
    numeric_features = X.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = X.select_dtypes(include=["object"]).columns.tolist()

    print("Numeric features:", numeric_features)
    print("Categorical features:", categorical_features)

    preprocess = ColumnTransformer(
        transformers=[
            ("num", "passthrough", numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ]
    )

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        class_weight="balanced",  
        random_state=42,
        n_jobs=-1,
    )

    clf = Pipeline(
        steps=[
            ("preprocess", preprocess),
            ("model", model),
        ]
    )

    # pipeline, numeric_features, categorical_features
    return clf, numeric_features, categorical_features

def train_and_evaluate(df: pd.DataFrame) -> Pipeline:
    y = df["is_default"]
    X = df.drop(columns=["is_default", "loan_id", "borrower_id", "loan_status"])

    clf, _, _ = build_pipeline(X)

    # test split : training 80%, test 20%
    X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=0.2,stratify=y,random_state=42,)

    print(f"Train size: {X_train.shape}")
    print(f"Test size: {X_test.shape}\n")

    print("=== Training model ===")
    clf.fit(X_train, y_train)
    print("Training complete.\n")

    print("=== Evaluation on TEST set ===")
    y_pred = clf.predict(X_test)

    print("\nClassification report:")
    print(classification_report(y_test, y_pred))

    print("Confusion matrix (rows=true, cols=pred):")
    print(confusion_matrix(y_test, y_pred))

    return clf #fitted pipeline

def save_model(pipeline: Pipeline, path: Path = MOD_PATH) -> None:
    Path("artifacts").mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, path)
    print(f"\nSaved model to: {path.resolve()}")

def main() -> None:
    df = load_training_data()
    pipeline = train_and_evaluate(df)
    save_model(pipeline)

if __name__ == "__main__":
    main()
