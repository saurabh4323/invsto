
import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/tickerdb")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# SQLAlchemy model
class TickerDataModel(Base):
    __tablename__ = "ticker_data"
    
    id = Column(Integer, primary_key=True, index=True)
    datetime = Column(DateTime, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Integer, nullable=False)

# Create tables
Base.metadata.create_all(bind=engine)

def import_csv_data(file_path):
    # Read CSV file
    df = pd.read_csv(file_path)
    
    # Convert 'datetime' column to datetime type
    df['datetime'] = pd.to_datetime(df['datetime'])
    
    # Create database session
    db = SessionLocal()
    
    try:
        # Insert data into database
        records = []
        for _, row in df.iterrows():
            record = TickerDataModel(
                datetime=row['datetime'],
                open=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=row['volume']
            )
            records.append(record)
        
        db.add_all(records)
        db.commit()
        print(f"Successfully imported {len(records)} records")
    except Exception as e:
        db.rollback()
        print(f"Error importing data: {str(e)}")
    finally:
        db.close()

if __name__ == "__main__":
    # Download the CSV file from Google Sheets and specify the file path
    file_path = "ticker_data.csv"
    import_csv_data(file_path)
