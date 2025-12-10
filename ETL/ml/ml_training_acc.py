"""
Goal: given a loan application, predict whether the loan will default.
Target: Will the loan default? good/bad 0/1

    -- loan_status values
bad -- Charged Off, Default, Late (31-120 days)
good -- Fully Paid Current

dropped -- empty/NULL, Does not meet the credit policy. Status:, In Grace Period, Late (16-30 days)
"""

"""
OUTPUT/PRINT

Created view: accepted_loans_ml_training
accepted_loans_ml_training -> total=81460, defaults(1)=10095, non_defaults(0)=71365

"""

from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

VIEW_NM = "accepted_loans_ml_training"

def create_accepted_loans_training_view(engine: Optional[Engine] = None) -> None:
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    ddl = text(
        f"""
        DROP VIEW IF EXISTS {VIEW_NM};

        CREATE VIEW {VIEW_NM} AS
        SELECT
            -- ids, primary key
            al.loan_id,
            al.borrower_id,

            CASE
                WHEN al.loan_status IN ('Charged Off', 'Default', 'Late (31-120 days)')
                    THEN 1
                WHEN al.loan_status IN ('Fully Paid', 'Current')
                    THEN 0
                ELSE NULL
            END AS is_default,

            -- numerics
            al.loan_amnt,
            al.installment,
            al.dti,
            al.income,
            al.term_months,

            -- cats
            al.purpose
        FROM Accepted_Loans al
        -- drops
        WHERE al.loan_status IN (
            'Charged Off',
            'Default',
            'Late (31-120 days)',
            'Fully Paid',
            'Current'
        );
        """
    )

    with engine.connect() as conn:
        conn.execute(ddl)
        conn.commit()

    print(f"Created view: {VIEW_NM}")

# row counts for total/pos/nega
def preview_training_counts(engine: Optional[Engine] = None) -> None:
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    with engine.connect() as conn:
        total = conn.execute(
            text(f"SELECT COUNT(*) FROM {VIEW_NM}")
        ).scalar()

        positives = conn.execute(
            text(f"SELECT COUNT(*) FROM {VIEW_NM} WHERE is_default = 1")
        ).scalar()

        negatives = conn.execute(
            text(f"SELECT COUNT(*) FROM {VIEW_NM} WHERE is_default = 0")
        ).scalar()

    print(
        f"{VIEW_NM} -> total={total}, "
        f"defaults(1)={positives}, non_defaults(0)={negatives}"
    )

if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2:///credit_risk")
    create_accepted_loans_training_view(engine)
    preview_training_counts(engine)
