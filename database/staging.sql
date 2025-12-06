DROP TABLE IF EXISTS staging_accepted_kaggle CASCADE;
DROP TABLE IF EXISTS staging_rejected_kaggle CASCADE;
DROP TABLE IF EXISTS staging_accepted_hdma CASCADE;
DROP TABLE IF EXISTS staging_rejected_hdma CASCADE;

CREATE TABLE staging_accepted_kaggle (
    id                              INTEGER,      
    loan_amnt                       NUMERIC(12, 2),
    funded_amnt                     NUMERIC(12, 2),
    term                            TEXT,         
    int_rate                        NUMERIC(5, 2), 
    installment                     NUMERIC(12, 2),
    annual_inc                      NUMERIC(14, 2),
    dti                             NUMERIC(6, 2),
    delinq_2yrs                     SMALLINT,
    fico_range_low                  SMALLINT,
    fico_range_high                 SMALLINT,
    inq_last_6mths                  SMALLINT,
    open_acc                        SMALLINT,
    total_acc                       SMALLINT,
    revol_bal                       NUMERIC(14, 2),
    revol_util                      NUMERIC(5, 2),
    pub_rec_bankruptcies            SMALLINT,

    home_ownership                  TEXT,
    verification_status             TEXT,
    loan_status                     TEXT,
    purpose                         TEXT,
    application_type                TEXT
);

CREATE TABLE staging_rejected_kaggle (
    "Amount Requested"              NUMERIC(12, 2),
    "Application Date"              TEXT,       
    "Loan Title"                    TEXT,
    "Debt-To-Income Ratio"          TEXT         
);

CREATE TABLE staging_accepted_hdma (
    loan_amount                     NUMERIC(12, 2),
    loan_term                       SMALLINT,   
    interest_rate                   NUMERIC(5, 2), 

    income                          NUMERIC(14, 2),
    debt_to_income_ratio            TEXT,         
    applicant_credit_score_type     TEXT,
    "co-applicant_credit_score_type"  TEXT,

    activity_year                   SMALLINT,
    action_taken                    SMALLINT,
    preapproval                     SMALLINT,
    loan_to_value_ratio             NUMERIC(5, 2),
    total_loan_costs                NUMERIC(12, 2),
    derived_loan_product_type       TEXT,
    loan_purpose                    TEXT
);

CREATE TABLE staging_rejected_hdma (
    activity_year                   SMALLINT,
    action_taken                    SMALLINT,      
    preapproval                     SMALLINT,
    loan_purpose                    TEXT,
    loan_amount                     NUMERIC(12, 2),
    loan_term                       SMALLINT,
    loan_to_value_ratio             NUMERIC(5, 2),
    income                          NUMERIC(14, 2),
    debt_to_income_ratio            TEXT,        
    derived_loan_product_type       TEXT,
    applicant_credit_score_type     TEXT,
    "co-applicant_credit_score_type" TEXT,
    "denial_reason-1"               TEXT
);