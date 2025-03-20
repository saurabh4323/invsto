
import unittest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from main import app, get_db, Base, TickerDataModel
from datetime import datetime
import pandas as pd
import numpy as np


SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Override the get_db dependency
def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()

app.dependency_overrides[get_db] = override_get_db

client = TestClient(app)

class TestTickerDataAPI(unittest.TestCase):
    def setUp(self):
        # Create tables
        Base.metadata.create_all(bind=engine)
        
        # Insert test data
        db = TestingSessionLocal()
        test_data = [
            TickerDataModel(datetime=datetime(2023, 1, 1), open=100.0, high=110.0, low=95.0, close=105.0, volume=1000),
            TickerDataModel(datetime=datetime(2023, 1, 2), open=105.0, high=115.0, low=100.0, close=110.0, volume=1200),
            TickerDataModel(datetime=datetime(2023, 1, 3), open=110.0, high=120.0, low=105.0, close=115.0, volume=1400),
            TickerDataModel(datetime=datetime(2023, 1, 4), open=115.0, high=125.0, low=110.0, close=120.0, volume=1600),
            TickerDataModel(datetime=datetime(2023, 1, 5), open=120.0, high=130.0, low=115.0, close=125.0, volume=1800),
            TickerDataModel(datetime=datetime(2023, 1, 6), open=125.0, high=135.0, low=120.0, close=130.0, volume=2000),
            TickerDataModel(datetime=datetime(2023, 1, 7), open=130.0, high=140.0, low=125.0, close=135.0, volume=2200),
            TickerDataModel(datetime=datetime(2023, 1, 8), open=135.0, high=145.0, low=130.0, close=140.0, volume=2400),
            TickerDataModel(datetime=datetime(2023, 1, 9), open=140.0, high=150.0, low=135.0, close=145.0, volume=2600),
            TickerDataModel(datetime=datetime(2023, 1, 10), open=145.0, high=155.0, low=140.0, close=150.0, volume=2800),
            TickerDataModel(datetime=datetime(2023, 1, 11), open=150.0, high=160.0, low=145.0, close=155.0, volume=3000),
            TickerDataModel(datetime=datetime(2023, 1, 12), open=155.0, high=165.0, low=150.0, close=160.0, volume=3200),
            TickerDataModel(datetime=datetime(2023, 1, 13), open=160.0, high=170.0, low=155.0, close=165.0, volume=3400),
            TickerDataModel(datetime=datetime(2023, 1, 14), open=165.0, high=175.0, low=160.0, close=170.0, volume=3600),
            TickerDataModel(datetime=datetime(2023, 1, 15), open=170.0, high=180.0, low=165.0, close=175.0, volume=3800),
            TickerDataModel(datetime=datetime(2023, 1, 16), open=175.0, high=185.0, low=170.0, close=180.0, volume=4000),
            TickerDataModel(datetime=datetime(2023, 1, 17), open=180.0, high=190.0, low=175.0, close=185.0, volume=4200),
            TickerDataModel(datetime=datetime(2023, 1, 18), open=185.0, high=195.0, low=180.0, close=190.0, volume=4400),
            TickerDataModel(datetime=datetime(2023, 1, 19), open=190.0, high=200.0, low=185.0, close=195.0, volume=4600),
            TickerDataModel(datetime=datetime(2023, 1, 20), open=195.0, high=205.0, low=190.0, close=200.0, volume=4800),
            TickerDataModel(datetime=datetime(2023, 1, 21), open=200.0, high=210.0, low=195.0, close=205.0, volume=5000),
            TickerDataModel(datetime=datetime(2023, 1, 22), open=205.0, high=215.0, low=200.0, close=210.0, volume=5200),
        ]
        db.add_all(test_data)
        db.commit()
        db.close()

    def tearDown(self):
        # Drop tables
        Base.metadata.drop_all(bind=engine)

    def test_get_ticker_data(self):
        response = client.get("/data")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        self.assertIn("datetime", data[0])
        self.assertIn("open", data[0])
        self.assertIn("high", data[0])
        self.assertIn("low", data[0])
        self.assertIn("close", data[0])
        self.assertIn("volume", data[0])

    def test_create_ticker_data(self):
        new_data = {
            "datetime": "2023-01-23T00:00:00",
            "open": 210.0,
            "high": 220.0,
            "low": 205.0,
            "close": 215.0,
            "volume": 5400
        }
        response = client.post("/data", json=new_data)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["open"], 210.0)
        self.assertEqual(data["high"], 220.0)
        self.assertEqual(data["low"], 205.0)
        self.assertEqual(data["close"], 215.0)
        self.assertEqual(data["volume"], 5400)

    def test_create_ticker_data_validation(self):
        # Test with invalid data (low > high)
        invalid_data = {
            "datetime": "2023-01-23T00:00:00",
            "open": 210.0,
            "high": 200.0,  # Invalid: high < open
            "low": 205.0,
            "close": 215.0,
            "volume": 5400
        }
        response = client.post("/data", json=invalid_data)
        self.assertEqual(response.status_code, 422)

    def test_strategy_performance(self):
        response = client.get("/strategy/performance?short_window=5&long_window=10")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn("short_window", data)
        self.assertIn("long_window", data)
        self.assertIn("total_trades", data)
        self.assertIn("final_value", data)
        self.assertIn("annualized_return", data)
        self.assertIn("sharpe_ratio", data)
        self.assertIn("period", data)

    def test_moving_average_calculation(self):
        # Test the moving average calculation logic
        close_prices = np.array([100, 105, 110, 115, 120, 125, 130, 135, 140, 145])
        short_window = 3
        long_window = 5
        
        # Calculate short-term moving average
        short_ma = np.convolve(close_prices, np.ones(short_window)/short_window, mode='valid')
        expected_short_ma = np.array([105, 110, 115, 120, 125, 130, 135, 140])
        np.testing.assert_array_almost_equal(short_ma, expected_short_ma)
        
        # Calculate long-term moving average
        long_ma = np.convolve(close_prices, np.ones(long_window)/long_window, mode='valid')
        expected_long_ma = np.array([110, 115, 120, 125, 130, 135])
        np.testing.assert_array_almost_equal(long_ma, expected_long_ma)

if __name__ == "__main__":
    unittest.main()
