"""
Goal: given a loan application, predict whether the loan will default.
Target: Will the loan default? good/bad 0/1

    -- loan_status values
bad -- Charged Off, Default, Late (31-120 days)
good -- Fully Paid Current

dropped -- empty/NULL, Does not meet the credit policy. Status:, In Grace Period, Late (16-30 days)
"""
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

VIEW_NM = "accepted_loans_ml_training"

def create_accepted_loans_training_view(engine: Optional[Engine] = None) -> None:
    # join acc loans /w borrowers on borrower_id
    # maps loan_status to is_default

    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    sql = text(
        f"""
        DROP VIEW IF EXISTS {VIEW_NM};

        CREATE VIEW {VIEW_NM} AS
        SELECT
            -- identifiers
            al.loan_id,
            al.borrower_id,

            -- loan
            al.loan_amnt,
            al.funded_amnt,
            al.term_months,
            al.int_rate,
            al.installment,
            al.loan_status,
            al.purpose,
            al.application_type,

            -- HDMA features, can be null for kaggle
            al.activity_year,
            al.action_taken,
            al.preapproval,
            al.loan_to_value_ratio,
            al.total_loan_costs,
            al.derived_loan_product_type,
            al.loan_purpose,

            -- borrower 
            b.annual_inc,
            b.dti,
            b.delinq_2yrs,
            b.fico_range_low,
            b.fico_range_high,
            b.inq_last_6mths,
            b.open_acc,
            b.total_acc,
            b.revol_bal,
            b.revol_util,
            b.pub_rec_bankruptcies,
            b.home_ownership,
            b.verification_status,
            b.income,
            b.debt_to_income_ratio,
            b.applicant_credit_score_type,
            b.co_applicant_credit_score_type,

            -- good/bad 0/1
            CASE
                WHEN al.loan_status IN (
                    'Charged Off',
                    'Default',
                    'Late (31-120 days)'
                ) THEN 1
                WHEN al.loan_status IN (
                    'Fully Paid',
                    'Current'
                ) THEN 0
            END AS is_default
        FROM Accepted_Loans al
        JOIN Borrowers b
          ON al.borrower_id = b.borrower_id
        WHERE
            -- drops
            NULLIF(TRIM(al.loan_status), '') IS NOT NULL
            AND al.loan_status NOT IN (
                'Does not meet the credit policy. Status:',
                'In Grace Period',
                'Late (16-30 days)'
            );
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)


def preview_training_counts(engine: Optional[Engine] = None) -> None:
    """
    Convenience helper: print how many rows (total / positives / negatives)
    are in the training view.
    """
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")


    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM {VIEW_NM}")).scalar()
        positives = conn.execute(text(f"SELECT COUNT(*) FROM {VIEW_NM} WHERE is_default = 1")).scalar()
        negatives = conn.execute(text(f"SELECT COUNT(*) FROM {VIEW_NM} WHERE is_default = 0")).scalar()

    print(
        f"{VIEW_NM} -> total={total}, "
        f"defaults(1)={positives}, non_defaults(0)={negatives}"
    )

if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2:///credit_risk")
    create_accepted_loans_training_view(engine)
    preview_training_counts(engine)
