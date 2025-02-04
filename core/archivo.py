from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey
import pandas as pd


DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db    
    finally:
        db.close()




def create_table_from_csv(df: pd.DataFrame, table_name: str):
    
    column_types = {
        "int64": Integer,
        "float64": String,
        "object": String,
        "datetime64[ns]": DateTime
    }
    columns = {col: Column(column_types[str(df[col].dtype)], nullable=True) for col in df.columns}
    columns["id"] = Column(Integer, primary_key=True, index=True)
    table = type(table_name, (Base,), {"__tablename__": table_name, **columns})
    
    Base.metadata.create_all(bind=engine)
    
            
    db = SessionLocal()
    for _, row in df.iterrows():
            record = table(**row.to_dict())
            db.add(record)
    db.commit()
    db.close()
    return table
