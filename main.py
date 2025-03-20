# main.py
import os
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import List, Optional
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

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

# Pydantic models for validation
class TickerDataBase(BaseModel):
    datetime: datetime
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: int = Field(..., ge=0)
    
    @validator('high')
    def high_must_be_greater_than_low_and_open(cls, v, values):
        if 'low' in values and v < values['low']:
            raise ValueError('high must be greater than or equal to low')
        if 'open' in values and v < values['open']:
            raise ValueError('high must be greater than or equal to open')
        return v
    
    @validator('low')
    def low_must_be_less_than_high_and_open(cls, v, values):
        if 'high' in values and v > values['high']:
            raise ValueError('low must be less than or equal to high')
        if 'open' in values and v > values['open']:
            raise ValueError('low must be less than or equal to open')
        return v

class TickerDataCreate(TickerDataBase):
    pass

class TickerData(TickerDataBase):
    id: int
    
    class Config:
        orm_mode = True

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="Ticker Data API")

# GET /data endpoint
@app.get("/data", response_model=List[TickerData])
def get_ticker_data(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    data = db.query(TickerDataModel).offset(skip).limit(limit).all()
    return data

# POST /data endpoint
@app.post("/data", response_model=TickerData, status_code=201)
def create_ticker_data(ticker_data: TickerDataCreate, db: Session = Depends(get_db)):
    db_ticker_data = TickerDataModel(**ticker_data.dict())
    db.add(db_ticker_data)
    db.commit()
    db.refresh(db_ticker_data)
    return db_ticker_data

# POST /data/batch endpoint to add multiple records
@app.post("/data/batch", response_model=List[TickerData], status_code=201)
def create_ticker_data_batch(ticker_data_list: List[TickerDataCreate], db: Session = Depends(get_db)):
    db_ticker_data_list = [TickerDataModel(**ticker_data.dict()) for ticker_data in ticker_data_list]
    db.add_all(db_ticker_data_list)
    db.commit()
    for db_ticker_data in db_ticker_data_list:
        db.refresh(db_ticker_data)
    return db_ticker_data_list

# Simple Trading Strategy endpoint
@app.get("/strategy/performance")
def get_strategy_performance(short_window: int = 5, long_window: int = 20, db: Session = Depends(get_db)):
    # Fetch all data
    data = db.query(TickerDataModel).order_by(TickerDataModel.datetime).all()
    
    if len(data) < long_window:
        raise HTTPException(status_code=400, detail=f"Not enough data. Need at least {long_window} data points.")
    
    # Convert to pandas DataFrame
    df = pd.DataFrame([(item.datetime, item.close) for item in data], columns=['datetime', 'close'])
    
    # Calculate moving averages
    df['short_ma'] = df['close'].rolling(window=short_window).mean()
    df['long_ma'] = df['close'].rolling(window=long_window).mean()
    
    # Generate signals
    df['signal'] = 0
    df.loc[df['short_ma'] > df['long_ma'], 'signal'] = 1  # Buy signal
    df.loc[df['short_ma'] < df['long_ma'], 'signal'] = -1  # Sell signal
    
    # Calculate returns
    df['returns'] = df['close'].pct_change()
    df['strategy_returns'] = df['signal'].shift(1) * df['returns']
    
    # Calculate cumulative returns
    df['cumulative_returns'] = (1 + df['returns']).cumprod() - 1
    df['strategy_cumulative_returns'] = (1 + df['strategy_returns']).cumprod() - 1
    
    # Calculate performance metrics
    final_value = df['strategy_cumulative_returns'].iloc[-1]
    annualized_return = (1 + final_value) ** (252 / len(df)) - 1  # Assuming 252 trading days
    sharpe_ratio = df['strategy_returns'].mean() / df['strategy_returns'].std() * np.sqrt(252)
    
    # Count trades
    df['trades'] = df['signal'].diff().abs()
    total_trades = df['trades'].sum() / 2  # Divide by 2 as each trade is counted twice (enter and exit)
    
    # Result
    result = {
        "short_window": short_window,
        "long_window": long_window,
        "total_trades": int(total_trades),
        "final_value": float(final_value),
        "annualized_return": float(annualized_return),
        "sharpe_ratio": float(sharpe_ratio),
        "period": {
            "start": df['datetime'].iloc[0].isoformat(),
            "end": df['datetime'].iloc[-1].isoformat()
        }
    }
    
    return result

# Helper function to import data from CSV
@app.post("/import/csv")
def import_csv(file_path: str, db: Session = Depends(get_db)):
    try:
        df = pd.read_csv(file_path)
        # Convert to datetime
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Insert into database
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
        
        return {"message": f"Successfully imported {len(records)} records"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error importing CSV: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
