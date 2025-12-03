-- psql -d postgres
-- CREATE DATABASE credit_risk;
-- \l -- to verify u created it
-- \q -- to exit

-- psql -d credit_risk
-- \dt -- list all tables
-- \d borrowers -- peek inside borrowers table

DROP INDEX IF EXISTS idx_accepted_loans_borrower;
DROP INDEX IF EXISTS idx_accepted_loans_status;
DROP INDEX IF EXISTS idx_rejected_borrower;

DROP TABLE IF EXISTS Borrowers;
DROP TABLE IF EXISTS Accepted_Loans;
DROP TABLE IF EXISTS Loan_Payments;
DROP TABLE IF EXISTS Rejected;

-- customers table
CREATE TABLE Borrowers (
    borrower_id           BIGSERIAL PRIMARY KEY,

    -- kaggle
    annual_inc            NUMERIC(14, 2),
    dti                   NUMERIC(6, 2),
    delinq_2yrs           SMALLINT, -- SMALLINT saves space over INTEGER
    fico_range_low        SMALLINT,
    fico_range_high       SMALLINT,
    inq_last_6mths        SMALLINT,
    open_acc              SMALLINT,
    total_acc             SMALLINT,
    revol_bal             NUMERIC(14, 2),
    revol_util            NUMERIC(5, 2),
    pub_rec_bankruptcies  SMALLINT,
    home_ownership        TEXT,
    verification_status   TEXT,

    created_at            TIMESTAMPTZ DEFAULT now() -- records when borrower is entered into system
);

-- accepted table
CREATE TABLE Accepted_Loans (
    loan_id               INTEGER PRIMARY KEY,           -- from kaggle `id`

    borrower_id           INTEGER NOT NULL REFERENCES borrowers(borrower_id),

    -- kaggle - num
    loan_amnt             NUMERIC(12, 2) NOT NULL,
    funded_amnt           NUMERIC(12, 2),
    term_months           SMALLINT NOT NULL, -- from kaggle "term"
    int_rate              NUMERIC(5, 2),
    installment           NUMERIC(12, 2),
    -- kaggle - cat
    loan_status           VARCHAR(40),                  -- "Fully Paid", "Charged Off", etc.
    purpose               VARCHAR(50),
    application_type      VARCHAR(30),

    created_at            TIMESTAMPTZ DEFAULT now()
);

-- loans table
CREATE TABLE Loan_Payments (
    payment_id            BIGSERIAL PRIMARY KEY,

    loan_id               INTEGER NOT NULL REFERENCES accepted_loans(loan_id),

    -- fill in

    created_at            TIMESTAMPTZ DEFAULT now()
);

-- rejected table
CREATE TABLE Rejected ( 
    application_id        BIGSERIAL PRIMARY KEY,
    borrower_id           INTEGER REFERENCES borrowers(borrower_id),

    -- kaggle
    amount_requested      NUMERIC(12, 2), -- map to "Amount Requested"
    application_date      DATE, -- map to "Application Date"
    loan_title            TEXT, -- map to "Loan Title"
    dti                   NUMERIC(6, 2), -- map to "Debt-To-Income Ratio"

    created_at            TIMESTAMPTZ DEFAULT now()
);

-- ml table?

-- indexing
CREATE INDEX idx_accepted_loans_borrower ON Accepted_Loans (borrower_id);
CREATE INDEX idx_accepted_loans_status ON Accepted_Loans (loan_status);
CREATE INDEX idx_rejected_borrower ON Rejected (borrower_id);