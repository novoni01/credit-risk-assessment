#python -m ETL.transformation.validation_loader
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd
from typing import Optional, Dict

def create_valid_accepted_kaggle(engine: Engine) -> None:
    sql = text("""
        DROP VIEW IF EXISTS valid_accepted_kaggle;
                
        CREATE OR REPLACE VIEW valid_accepted_kaggle AS
        SELECT
            id,
            loan_amnt,
            funded_amnt,
            term_months,
            int_rate,
            installment,
            annual_inc,
            dti,
            delinq_2yrs,
            fico_range_low,
            fico_range_high,
            inq_last_6mths,
            open_acc,
            total_acc,
            revol_bal,
            revol_util,
            pub_rec_bankruptcies,
            home_ownership, 
            verification_status,
            loan_status,

        CASE
            WHEN purpose = 'credit_card' THEN 'credit_card'
            WHEN purpose = 'debt_consolidation' THEN 'debt_consolidation'
            WHEN purpose = 'major_purchase' THEN 'major_purchase'
            WHEN purpose = 'medical' THEN 'medical'
            WHEN purpose IN ('car', 'home_improvement', 'house', 'moving', 'renewable_energy', 'small_business', 'vacation', 'wedding') THEN 'major_purchase'
            ELSE 'other'
        END AS purpose,

        application_type

        FROM staging_accepted_kaggle
        WHERE
            id IS NOT NULL
            AND loan_amnt > 1000
            AND term_months IN ('36','60')
            AND funded_amnt <= loan_amnt
            AND int_rate BETWEEN 0 AND 30
            AND installment > 0
            AND installment < funded_amnt
            AND dti BETWEEN 0 AND 35
            AND delinq_2yrs BETWEEN 0 AND 20
            AND annual_inc BETWEEN 0 AND 3000000
            AND fico_range_low BETWEEN 300 AND 850 
            AND fico_range_high BETWEEN 300 AND 850
            AND fico_range_low <= fico_range_high
            AND inq_last_6mths BETWEEN 0 AND 20
            AND open_acc BETWEEN 0 AND 15
            AND total_acc BETWEEN 0 AND 20
            AND revol_bal >= 0
            AND revol_util BETWEEN 0 AND 100
            AND pub_rec_bankruptcies BETWEEN 0 AND 5
            AND home_ownership NOT IN ('NONE', 'OTHER')
            AND loan_status IS NOT NULL
            AND purpose IS NOT NULL;
    """)
    with engine.begin() as conn:
        conn.execute(sql)

def create_valid_rejected_kaggle(engine: Engine) -> None:
    sql = text("""
    DROP VIEW IF EXISTS valid_rejected_kaggle;           

    CREATE OR REPLACE VIEW valid_rejected_kaggle AS
    SELECT
        "Amount Requested",
        "Application Date",
        "Loan Title",
        "Debt-To-Income Ratio"
        
    FROM staging_rejected_kaggle
    WHERE 
        "Amount Requested" IS NOT NULL
        AND "Amount Requested" > 0
        AND "Application Date" IS NOT NULL
        AND "Loan Title" IS NOT NULL
        AND "Debt-To-Income Ratio" <> ''
        AND NULLIF("Debt-To-Income Ratio", '')::NUMERIC BETWEEN 0 AND 35;    
    
    """)

    with engine.begin() as conn:
        conn.execute(sql)

def create_valid_accepted_hdma(engine: Engine) -> None:
    sql = text("""
    DROP VIEW IF EXISTS valid_accepted_hdma;           

    CREATE OR REPLACE VIEW valid_accepted_hdma AS
    SELECT
        loan_amount,
        loan_term, 
        interest_rate,
        income,
        debt_to_income_ratio,         

        CASE
            WHEN applicant_credit_score_type IN ('1','2') THEN 'Equifax'
            WHEN applicant_credit_score_type IN ('3','4') THEN 'FICO'
            WHEN applicant_credit_score_type IN ('5','6') THEN 'VantageScore'
            WHEN applicant_credit_score_type = '7' THEN 'Multiple Models'
            WHEN applicant_credit_score_type = '8' THEN 'Other Model'
            WHEN applicant_credit_score_type = '9' THEN 'Not Applicable'
            ELSE 'Exempt'
        END AS applicant_credit_score_type,

        CASE
            WHEN co_applicant_credit_score_type IN ('1','2') THEN 'Equifax'
            WHEN co_applicant_credit_score_type IN ('3','4','11') THEN 'FICO'
            WHEN co_applicant_credit_score_type IN ('5','6') THEN 'VantageScore'
            WHEN co_applicant_credit_score_type = '7' THEN 'Multiple Models'
            WHEN co_applicant_credit_score_type = '8' THEN 'Other Model'
            WHEN co_applicant_credit_score_type = '9' THEN 'Not Applicable'
            WHEN co_applicant_credit_score_type = '10' THEN 'No co-applicant'
            ELSE 'Exempt'
        END AS co_applicant_credit_score_type,

        activity_year,
        action_taken,
        preapproval,
        loan_to_value_ratio,
        total_loan_costs,
        derived_loan_product_type,
        loan_purpose

    FROM staging_accepted_hdma
    WHERE
        loan_amount IS NOT NULL
        AND loan_amount > 0
        AND loan_term > 0
        AND income BETWEEN 0 AND 3000000
        AND debt_to_income_ratio <> ''
        AND debt_to_income_ratio IS NOT NULL
        AND applicant_credit_score_type IN ('1','2','3','4','5','6','7','8','9','11','1111')
        AND co_applicant_credit_score_type IN ('1','2','3','4','5','6','7','8','9','10','11','1111')
        AND activity_year = 2023
        AND action_taken = 1
        AND preapproval IN (1,2)
        AND loan_to_value_ratio BETWEEN 0 AND 130
        AND derived_loan_product_type IN (
            'Conventional:First Lien',
            'Conventional:Subordinate Lien',
            'FHA:First Lien',
            'FHA:Subordinate Lien',
            'VA:First Lien',
            'VA:Subordinate Lien',
            'FSA/RHS:First Lien',
            'FSA/RHS:Subordinate Lien'
        )
        AND loan_purpose <> ''
        AND loan_purpose IS NOT NULL;
    """)

    with engine.begin() as conn:
        conn.execute(sql)

def create_valid_rejected_hdma(engine: Engine) -> None:
    sql = text("""
    DROP VIEW IF EXISTS valid_rejected_hdma;           

    CREATE OR REPLACE VIEW valid_rejected_hdma AS
    SELECT
        activity_year,
        action_taken,      
        preapproval,
        loan_purpose,
        loan_amount,
        loan_term,
        loan_to_value_ratio,
        income,
        debt_to_income_ratio,   
        derived_loan_product_type,

        CASE
            WHEN applicant_credit_score_type IN ('1','2') THEN 'Equifax'
            WHEN applicant_credit_score_type IN ('3','4') THEN 'FICO'
            WHEN applicant_credit_score_type IN ('5','6') THEN 'VantageScore'
            WHEN applicant_credit_score_type = '7' THEN 'Multiple Models'
            WHEN applicant_credit_score_type = '8' THEN 'Other Model'
            WHEN applicant_credit_score_type = '9' THEN 'Not Applicable'
            ELSE 'Exempt'
        END AS applicant_credit_score_type,

        CASE
            WHEN co_applicant_credit_score_type IN ('1','2') THEN 'Equifax'
            WHEN co_applicant_credit_score_type IN ('3','4','11') THEN 'FICO'
            WHEN co_applicant_credit_score_type IN ('5','6') THEN 'VantageScore'
            WHEN co_applicant_credit_score_type = '7' THEN 'Multiple Models'
            WHEN co_applicant_credit_score_type = '8' THEN 'Other Model'
            WHEN co_applicant_credit_score_type = '9' THEN 'Not Applicable'
            WHEN co_applicant_credit_score_type = '10' THEN 'No co-applicant'
            ELSE 'Exempt'
        END AS co_applicant_credit_score_type,

        CASE
            WHEN denial_reason_1 = '1' THEN 'Debt-to-income ratio'
            WHEN denial_reason_1 = '2' THEN 'Employment history'
            WHEN denial_reason_1 = '3' THEN 'Credit history'
            WHEN denial_reason_1 = '4' THEN 'Collateral'
            WHEN denial_reason_1 = '5' THEN 'Insufficient cash (downpayment, closing costs)'
            WHEN denial_reason_1 = '6' THEN 'Unverifiable information'
            WHEN denial_reason_1 = '7' THEN 'Credit application incomplete'
            WHEN denial_reason_1 = '8' THEN 'Mortgage insurance denied'
            WHEN denial_reason_1 IN ('9', '10') THEN 'Other'
        END AS denial_reason_1

    FROM staging_rejected_hdma
    WHERE 
        activity_year = 2023
        AND action_taken = 3     
        AND preapproval IN (1,2)
        AND loan_purpose <> ''
        AND loan_purpose IS NOT NULL
        AND loan_amount > 0
        AND loan_term > 0
        AND loan_to_value_ratio > 0
        AND income BETWEEN 0 AND 3000000
        AND debt_to_income_ratio <> ''
        AND debt_to_income_ratio IS NOT NULL
        AND derived_loan_product_type IS NOT NULL
        AND derived_loan_product_type IN (
            'Conventional:First Lien',
            'Conventional:Subordinate Lien',
            'FHA:First Lien',
            'FHA:Subordinate Lien',
            'VA:First Lien',
            'VA:Subordinate Lien',
            'FSA/RHS:First Lien',
            'FSA/RHS:Subordinate Lien'
        )
        AND applicant_credit_score_type IN ('1','2','3','4','5','6','7','8','9','1111')
        AND co_applicant_credit_score_type IN ('1','2','3','4','5','6','7','8','9','10','1111')
        AND denial_reason_1 IN ('1','2','3','4','5','6','7','8','9','10');
    """)