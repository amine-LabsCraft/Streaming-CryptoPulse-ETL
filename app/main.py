"""
CryptoPulse ETL - Main application module
"""

from fastapi import FastAPI
from config.settings import *

app = FastAPI(
    title="CryptoPulse ETL",
    description="ETL pipeline for crypto market data from Binance to MySQL/MongoDB/Power BI",
    version="1.0.0",
)

@app.get("/")
async def root():
    return {"message": "CryptoPulse ETL is running", "status": "active"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "symbols": SYMBOLS}