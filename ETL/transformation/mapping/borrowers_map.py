from sqlalchemy import text
from sqlalchemy.engine import Engine

def map_borrowers_from_kaggle_accepted(engine: Engine) -> None:
    sql = text("""
        INSERT INTO Borrowers (
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
            verification_status
        )
        SELECT DISTINCT
            sa.annual_inc,
            sa.dti,
            sa.delinq_2yrs,
            sa.fico_range_low,
            sa.fico_range_high,
            sa.inq_last_6mths,
            sa.open_acc,
            sa.total_acc,
            sa.revol_bal,
            sa.revol_util,
            sa.pub_rec_bankruptcies,
            sa.home_ownership,
            sa.verification_status
        FROM staging_accepted_kaggle sa
    """)

    with engine.begin() as conn:
        conn.execute(sql)

def map_borrowers_from_hdma_accepted(engine: Engine) -> None:
    # debt_to_income_ratio staging is TEXT --> strip non-numeric chars and cast to numeric
    sql = text("""
        INSERT INTO Borrowers (
            income,
            debt_to_income_ratio,
            applicant_credit_score_type,
            co_applicant_credit_score_type
        )
        SELECT DISTINCT
            sh.income,
            NULLIF(REGEXP_REPLACE(sh.debt_to_income_ratio, '[^0-9\\.]', '', 'g'),'')::NUMERIC,
            sh.applicant_credit_score_type,
            sh.co_applicant_credit_score_type
        FROM staging_accepted_hdma sh;
    """)

    with engine.begin() as conn:
        conn.execute(sql)

def map_all_borrowers(engine: Engine) -> None:
    map_borrowers_from_kaggle_accepted(engine)
    map_borrowers_from_hdma_accepted(engine)