from sqlalchemy import text
from sqlalchemy.engine import Engine

def map_accepted_loans_from_kaggle(engine: Engine) -> None:
    # skips any rows whose loan_id is already present in Accepted_Loans 
    #ON CONFLICT (loan_id) DO NOTHING makes it safe to rerun --> existing rows are skipped, duplicates ignored
    sql = text(
        """
        INSERT INTO Accepted_Loans (
            loan_id,
            borrower_id,
            loan_amnt,
            funded_amnt,
            term_months,
            int_rate,
            installment,
            dti,
            loan_status,
            purpose,
            application_type,
            activity_year,
            action_taken,
            preapproval,
            loan_to_value_ratio,
            total_loan_costs,
            derived_loan_product_type,
            loan_purpose
        )
        SELECT
            sa.id AS loan_id,
            b.borrower_id,
            sa.loan_amnt,
            sa.funded_amnt,
            NULLIF(
                REGEXP_REPLACE(sa.term_months, '[^0-9]', '', 'g'),
                ''
            )::SMALLINT                                  AS term_months,
            sa.int_rate,
            sa.installment,
            sa.dti,
            LEFT(sa.loan_status, 40)                     AS loan_status,
            LEFT(sa.purpose, 50)                         AS purpose,
            LEFT(sa.application_type, 30)                AS application_type,
            NULL::SMALLINT                               AS activity_year,
            NULL::SMALLINT                               AS action_taken,
            NULL::SMALLINT                               AS preapproval,
            NULL::NUMERIC(5,2)                           AS loan_to_value_ratio,
            NULL::NUMERIC(12,2)                          AS total_loan_costs,
            NULL::TEXT                                   AS derived_loan_product_type,
            NULL::TEXT                                   AS loan_purpose
        FROM valid_accepted_kaggle sa
        JOIN Borrowers b
          ON b.annual_inc            = sa.annual_inc
         AND b.dti                   = sa.dti
         AND b.delinq_2yrs           = sa.delinq_2yrs
         AND b.fico_range_low        = sa.fico_range_low
         AND b.fico_range_high       = sa.fico_range_high
         AND b.inq_last_6mths        = sa.inq_last_6mths
         AND b.open_acc              = sa.open_acc
         AND b.total_acc             = sa.total_acc
         AND b.revol_bal             = sa.revol_bal
         AND b.revol_util            = sa.revol_util
         AND b.pub_rec_bankruptcies  = sa.pub_rec_bankruptcies
         AND b.home_ownership        = sa.home_ownership
         AND b.verification_status   = sa.verification_status
        ON CONFLICT (loan_id) DO NOTHING;
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)

def map_accepted_loans_from_hdma(engine: Engine) -> None:
    """
        loan_amount      -> loan_amnt
        loan_term        -> term_months
        interest_rate    -> int_rate
    """

    sql = text(
        """
        WITH cleaned_hdma AS (
            SELECT
                sh.*,
                NULLIF(REGEXP_REPLACE(sh.debt_to_income_ratio, '[^0-9\\.]', '', 'g'),'')::NUMERIC(6,2) AS dti_clean,
                NULLIF(REGEXP_REPLACE(CAST(sh.loan_amount AS TEXT), '[^0-9\\.]', '', 'g'),'')::NUMERIC(12,2) AS loan_amnt_clean
            FROM valid_accepted_hdma sh
        ),
        hdma_with_borrower AS (
            SELECT
                ch.*,
                b.borrower_id
            FROM cleaned_hdma ch
            JOIN Borrowers b
              ON b.income                       = ch.income
             AND b.debt_to_income_ratio         = ch.dti_clean
             AND b.applicant_credit_score_type  = ch.applicant_credit_score_type
             AND b.co_applicant_credit_score_type = ch.co_applicant_credit_score_type
        ),
        numbered AS (
            SELECT
                hw.*,
                (
                    SELECT COALESCE(MAX(al.loan_id), 0)
                    FROM Accepted_Loans al
                ) + ROW_NUMBER() OVER (
                    ORDER BY hw.activity_year, hw.loan_amnt_clean, hw.income
                ) AS new_loan_id
            FROM hdma_with_borrower hw
        )
        INSERT INTO Accepted_Loans (
            loan_id,
            borrower_id,
            dti,
            loan_amnt,
            funded_amnt,
            term_months,
            int_rate,
            installment,
            loan_status,
            purpose,
            application_type,
            activity_year,
            action_taken,
            preapproval,
            loan_to_value_ratio,
            total_loan_costs,
            derived_loan_product_type,
            loan_purpose
        )
        SELECT
            n.new_loan_id               AS loan_id,
            n.borrower_id,
            n.loan_amnt_clean           AS loan_amnt,
            NULL::NUMERIC(12,2)         AS funded_amnt,
            n.loan_term                 AS term_months,
            n.interest_rate             AS int_rate,
            NULL::NUMERIC(12,2)         AS installment,
            n.dti_clean                 AS dti,
            NULL::VARCHAR(40)           AS loan_status,
            NULL::VARCHAR(50)           AS purpose,
            NULL::VARCHAR(30)           AS application_type,
            n.activity_year,
            n.action_taken,
            n.preapproval,
            n.loan_to_value_ratio,
            n.total_loan_costs,
            n.derived_loan_product_type,
            n.loan_purpose
        FROM numbered n;
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)

def map_all_accepted_loans(engine: Engine) -> None:
    map_accepted_loans_from_kaggle(engine)
    map_accepted_loans_from_hdma(engine)
