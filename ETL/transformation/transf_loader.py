#python -m ETL.transformation.transf_loader

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ETL.transformation.mapping.borrowers_map import map_all_borrowers
from ETL.transformation.mapping.accepted_loans_map import map_all_accepted_loans
from ETL.transformation.mapping.rejected_map import map_all_rejected_loans

def run_transf_loader(engine: Engine | None = None) -> None:
    if engine is None:
        engine = create_engine("postgresql+psycopg2:///credit_risk")

    print("=== Mapping into Borrowers ===")
    map_all_borrowers(engine)

    print("=== Mapping into Accepted_Loans ===")
    map_all_accepted_loans(engine)

    print("=== Mapping into Rejected ===")
    map_all_rejected_loans(engine)

    print("=== transf_loader SUCCESS!! ===")


if __name__ == "__main__":
    engine = create_engine("postgresql+psycopg2:///credit_risk")
    run_transf_loader(engine)
