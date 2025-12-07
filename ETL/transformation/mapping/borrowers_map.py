# map related fields from kaggle and hdma accepted datasets

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://localhost/credit_risk")
