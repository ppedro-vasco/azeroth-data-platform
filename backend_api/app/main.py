from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

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