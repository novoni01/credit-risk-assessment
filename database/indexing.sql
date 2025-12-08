-- psql -d credit_risk -f database/indexing.sql

-- accepted_Loans
DROP INDEX IF EXISTS idx_accepted_loans_borrower;
DROP INDEX IF EXISTS idx_accepted_loans_status;
DROP INDEX IF EXISTS idx_accepted_loans_purpose;
DROP INDEX IF EXISTS idx_accepted_loans_application_type;
DROP INDEX IF EXISTS idx_accepted_loans_activity_year;

-- borrowers
DROP INDEX IF EXISTS idx_borrowers_dti;
DROP INDEX IF EXISTS idx_borrowers_annual_inc;
DROP INDEX IF EXISTS idx_borrowers_fico_range;
DROP INDEX IF EXISTS idx_borrowers_income;
DROP INDEX IF EXISTS idx_borrowers_credit_score_type;

-- rejected
DROP INDEX IF EXISTS idx_rejected_dataset_source;
DROP INDEX IF EXISTS idx_rejected_dti;
DROP INDEX IF EXISTS idx_rejected_loan_purpose;
DROP INDEX IF EXISTS idx_rejected_activity_year;

-- speed up joins to borrower features
CREATE INDEX idx_accepted_loans_borrower ON Accepted_Loans (borrower_id);

-- fast filtering by status 
CREATE INDEX idx_accepted_loans_status ON Accepted_Loans (loan_status);

-- segmenting by purpose
CREATE INDEX idx_accepted_loans_purpose ON Accepted_Loans (purpose);

-- frequent slicing by application type
CREATE INDEX idx_accepted_loans_application_type ON Accepted_Loans (application_type);

-- year-based analysis for hdma
CREATE INDEX idx_accepted_loans_activity_yearN Accepted_Loans (activity_year);

-- dti
CREATE INDEX idx_borrowers_dti ON Borrowers (dti);

-- annual inc
CREATE INDEX idx_borrowers_annual_inc ON Borrowers (annual_inc);

-- indexing for full fico range
CREATE INDEX idx_borrowers_fico_range ON Borrowers (fico_range_low, fico_range_high);

-- HDMA income 
CREATE INDEX idx_borrowers_income ON Borrowers (income);

-- credit score type(s) for HDMA borrowers
CREATE INDEX idx_borrowers_credit_score_type ON Borrowers (applicant_credit_score_type, co_applicant_credit_score_type);

-- for spliting Kaggle vs HDMA rejected loans
CREATE INDEX idx_rejected_dataset_source ON Rejected (dataset_source);

-- DTI on rejected
CREATE INDEX idx_rejected_dti ON Rejected (dti);

-- rejected by loan_purpose
CREATE INDEX idx_rejected_loan_purpose ON Rejected (loan_purpose);

-- year filters for rej hdma rows
CREATE INDEX idx_rejected_activity_year ON Rejected (activity_year);
