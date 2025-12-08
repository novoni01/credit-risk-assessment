from sqlalchemy import text
from sqlalchemy.engine import Engine


def map_rejected_from_kaggle(engine: Engine) -> None:
    """
    - dataset_source is set to 'kaggle'.
    - amount_requested     <- "Amount Requested"
    - application_date     <- "Application Date"::DATE (Postgres cast)
    - loan_title           <- "Loan Title"
    - dti                  <- cleaned numeric from "Debt-To-Income Ratio"
    """

    sql = text(
        """
        INSERT INTO Rejected (
            dataset_source,
            amount_requested,
            application_date,
            loan_title,
            dti,
            -- hdma-only NULL
            activity_year,
            action_taken,
            preapproval,
            loan_purpose,
            loan_amount,
            loan_term,
            loan_to_value_ratio,
            income,
            derived_loan_product_type,
            applicant_credit_score_type,
            co_applicant_credit_score_type,
            denial_reason_1
        )
        SELECT
            'kaggle' AS dataset_source,
            sr."Amount Requested"              AS amount_requested,
            sr."Application Date"::DATE        AS application_date,
            sr."Loan Title"                    AS loan_title,
            NULLIF(REGEXP_REPLACE(sr."Debt-To-Income Ratio", '[^0-9\\.]', '', 'g'),'')::NUMERIC(6,2) AS dti,
            -- hdma 
            NULL::SMALLINT                     AS activity_year,
            NULL::SMALLINT                     AS action_taken,
            NULL::SMALLINT                     AS preapproval,
            NULL::TEXT                         AS loan_purpose,
            NULL::NUMERIC(12,2)                AS loan_amount,
            NULL::SMALLINT                     AS loan_term,
            NULL::NUMERIC(10,2)                AS loan_to_value_ratio,
            NULL::NUMERIC(14,2)                AS income,
            NULL::TEXT                         AS derived_loan_product_type,
            NULL::TEXT                         AS applicant_credit_score_type,
            NULL::TEXT                         AS co_applicant_credit_score_type,
            NULL::TEXT                         AS denial_reason_1
        FROM staging_rejected_kaggle sr;
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)


def map_rejected_from_hdma(engine: Engine) -> None:
    """
    - dataset_source is set to 'hdma'.
    - dti comes from cleaned debt_to_income_ratio TEXT.
    """

    sql = text(
        """
        INSERT INTO Rejected (
            dataset_source,
            amount_requested,
            application_date,
            loan_title,
            dti,
            activity_year,
            action_taken,
            preapproval,
            loan_purpose,
            loan_amount,
            loan_term,
            loan_to_value_ratio,
            income,
            derived_loan_product_type,
            applicant_credit_score_type,
            co_applicant_credit_score_type,
            denial_reason_1
        )
        SELECT 
            'hdma' AS dataset_source,
            NULL::NUMERIC(12,2)                 AS amount_requested,
            NULL::DATE                          AS application_date,
            NULL::TEXT                          AS loan_title,
            NULLIF(REGEXP_REPLACE(srh.debt_to_income_ratio, '[^0-9\\.]', '', 'g'),'')::NUMERIC(6,2) AS dti,
            srh.activity_year,
            srh.action_taken,
            srh.preapproval,
            srh.loan_purpose,
            srh.loan_amount,
            srh.loan_term,
            srh.loan_to_value_ratio,
            srh.income,
            srh.derived_loan_product_type,
            srh.applicant_credit_score_type,
            srh.co_applicant_credit_score_type,
            srh.denial_reason_1
        FROM staging_rejected_hdma srh;
        """
    )

    with engine.begin() as conn:
        conn.execute(sql)

def map_all_rejected_loans(engine: Engine) -> None:
    map_rejected_from_kaggle(engine)
    map_rejected_from_hdma(engine)