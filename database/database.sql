-- psql -d postgres
-- CREATE DATABASE credit_risk;
-- \l -- to verify u created it
-- \q -- to exit

-- psql -d credit_risk
-- \dt -- list all tables
-- \d borrowers -- peek inside borrowers table

DROP INDEX IF EXISTS idx_accepted_loans_borrower;
DROP INDEX IF EXISTS idx_accepted_loans_status;

DROP TABLE IF EXISTS Borrowers CASCADE;
DROP TABLE IF EXISTS Accepted_Loans CASCADE;
DROP TABLE IF EXISTS Rejected CASCADE;

-- customers table
CREATE TABLE Borrowers (
    borrower_id                     BIGSERIAL PRIMARY KEY,

    -- kaggle
    annual_inc                      NUMERIC(14, 2),
    dti                             NUMERIC(6, 2),
    delinq_2yrs                     SMALLINT, -- SMALLINT saves space over INTEGER
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

    -- HDMA borrower fields (NEW)
    income                          NUMERIC(14,2),
    debt_to_income_ratio            NUMERIC(6,2),
    applicant_credit_score_type     TEXT,
    co_applicant_credit_score_type  TEXT,

    created_at                      TIMESTAMPTZ DEFAULT now() -- records when borrower is entered into system
);

-- accepted table
CREATE TABLE Accepted_Loans (
    loan_id                         INTEGER PRIMARY KEY,           -- from kaggle `id`

    borrower_id                     INTEGER NOT NULL REFERENCES borrowers(borrower_id),

    -- kaggle - num
    loan_amnt                       NUMERIC(12, 2) NOT NULL, -- map loan_amount from hdma accepted
    funded_amnt                     NUMERIC(12, 2),
    term_months                     SMALLINT NOT NULL, -- from kaggle "term" -- map loan_term from hdma accepted
    int_rate                        NUMERIC(5, 2), -- map interest_rate from hdma accepted
    installment                     NUMERIC(12, 2),
    -- kaggle - cat
    loan_status                     VARCHAR(40),                  
    purpose                         VARCHAR(50),
    application_type                VARCHAR(30),

    -- HDMA-specific fields (NEW)
    activity_year                   SMALLINT,
    action_taken                    SMALLINT,     
    preapproval                     SMALLINT,
    loan_to_value_ratio             NUMERIC(5,2),
    total_loan_costs                NUMERIC(12,2),
    derived_loan_product_type       TEXT,
    loan_purpose                    TEXT,         -- different from kaggle purpose

    created_at                      TIMESTAMPTZ DEFAULT now()
);

-- rejected table for lendingclub 
CREATE TABLE Rejected ( 
    application_id                  BIGSERIAL PRIMARY KEY,

    dataset_source                  TEXT, -- track which dataset the info is coming from / might delete this field later idk

    -- kaggle
    amount_requested                NUMERIC(12, 2), -- map to "Amount Requested"
    application_date                DATE, -- map to "Application Date"
    loan_title                      TEXT, -- map to "Loan Title"
    dti                             NUMERIC(6, 2), -- map to "Debt-To-Income Ratio" from both kaggle and hdma

    -- hdma rejected
    activity_year                   SMALLINT,        
    action_taken                    SMALLINT,    
    preapproval                     SMALLINT,     
    loan_purpose                    TEXT,    
    loan_amount                     NUMERIC(12, 2),
    loan_term                       SMALLINT,  
    loan_to_value_ratio             NUMERIC(5, 2),  
    income                          NUMERIC(14, 2),  
    derived_loan_product_type       TEXT,            
    applicant_credit_score_type     TEXT,     
    co_applicant_credit_score_type  TEXT,
    denial_reason_1                 TEXT, 

    created_at                      TIMESTAMPTZ DEFAULT now()
);

-- ml table?

-- indexing
CREATE INDEX idx_accepted_loans_borrower ON Accepted_Loans (borrower_id);
CREATE INDEX idx_accepted_loans_status ON Accepted_Loans (loan_status);