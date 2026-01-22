from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional

from .database import get_db
from . import models, schemas

app = FastAPI(title="Azeroth Data Platform API")

@app.get("/")
def read_root():
    return {
        "project": "Azeroth Data Platform",
        "status": "Online",
        "message": "Lok'tar Ogar! A API está de pé."
    }

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/db-check")
def db_test(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        return {"status": "success", "message": "Conexão com Postgres bem sucedida!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao conectar ao banco de dados: {str(e)}")
    
@app.get("/items/", response_model=List[schemas.ItemResponse])
def list_items(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    items = db.query(models.ItemDimension).offset(skip).limit(limit).all()
    return items

@app.get("/analytics/daily-summary", response_model=List[schemas.DailyMarketSummaryResponse])
def get_daily_market_summary(item_id: Optional[int] = None, db: Session = Depends(get_db)):
    query = db.query(models.GoldDailyMarketSummary)
    if item_id:
        query = query.filter(models.GoldDailyMarketSummary.item_id == item_id)

    return query.order_by(models.GoldDailyMarketSummary.snapshot_date.desc()).limit(100).all()

@app.get("/analytics/price-history", response_model=List[schemas.PriceHistoryResponse])
def get_price_history(item_id: int, limit: int = 48, db: Session = Depends(get_db)):
    return db.query(models.GoldPriceHistory)\
        .filter(models.GoldPriceHistory.item_id == item_id)\
        .order_by(models.GoldPriceHistory.snapshot_hour.desc())\
        .limit(limit).all()  

@app.get("/analytics/opportunities", response_model=List[schemas.MarketOpportunityResponse])
def get_opportunities(recommendation: Optional[str] = None, db: Session = Depends(get_db)):
    query = db.query(models.GoldPriceOpportunities)
    if recommendation:
        query = query.filter(models.GoldPriceOpportunities.recommendation == recommendation.upper())

    return query.order_by(models.GoldMarketOpportunities.z_score.asc()).all()

@app.get("/analytics/demand", response_model=List[schemas.ItemDemandResponse])
def get_item_demand(item_id: Optional[int] = None, db: Session = Depends(get_db)):
    query = db.query(models.GoldItemDemand)
    if item_id:
        query = query.filter(models.GoldItemDemand.item_id == item_id)

    return query.order_by(models.GoldItemDemand.snapshot_date.desc()).limit(100).all()

@app.get("/analytics/concentration", response_model=List[schemas.MarketConcentrationResponse])
def get_market_concentration(status: Optional[int] = None, db: Session = Depends(get_db)):
    query = db.query(models.GoldMarketConcentration)
    if status:
        query = query.filter(models.GoldMarketConcentration.market_status == status)
    
    return query.order_by(models.GoldMarketConcentration.floor_concentration_pct.desc()).limit(100).all()

@app.get("/analytics/market-index", response_model=List[schemas.MarketIndexResponse])
def get_market_index(limit: int = 30, db: Session = Depends(get_db)):
    return db.query(models.GoldMarketIndex)\
        .order_by(models.GoldMarketIndex.snapshot_date.desc())\
        .limit(limit).all()