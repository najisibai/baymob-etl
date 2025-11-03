import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL','postgresql+psycopg2://baymob:baymob@localhost:5432/baymob')
ENGINE = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
