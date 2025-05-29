from fastapi import FastAPI, APIRouter, Depends, Query, HTTPException, Request
from sqlalchemy.orm import Session, selectinload
from fastapi.routing import APIRoute
from typing import List
from database import get_db
from models import Transaction, User
from datetime import date
from schemas import TransactionSchema, TransactionFullSchema, UserSchema, PaginationResponse, RouteInfoSchema

app = FastAPI()
api = APIRouter(prefix="/api")  

@api.get(
  "/", 
  response_model=List[RouteInfoSchema],
  summary="List all API routes"
)
def read_root(request: Request):
  route_list = []
  for route in request.app.routes:
    if isinstance(route, APIRoute):
      route_list.append(RouteInfoSchema(
        path=route.path,
        name=route.name,
        methods=sorted(route.methods - {"HEAD", "OPTIONS"})
      ))
  return route_list

@api.get(
  "/transactions/{tx_id}", 
  response_model=TransactionFullSchema,
  summary="Get a specific transaction with the client info"
)
def get_transaction(tx_id: int, db: Session = Depends(get_db)):
  tx = (
    db.query(Transaction)
      .options(selectinload(Transaction.client))  # eager load the user
      .filter(Transaction.id == tx_id)
      .one_or_none()
  )
  
  if not tx:
    raise HTTPException(status_code=404, detail="Not found")
  return tx


@api.get(
  "/transactions/",
  response_model=PaginationResponse[TransactionSchema],
  summary="List transactions with pagination"
)
def get_transactions(
  request: Request,
  page: int = Query(1, ge=1, description="Page number (starting from 1)"),
  limit: int = Query(50, ge=1, le=1000, description="Page size"),
  db: Session = Depends(get_db)
):
  offset = (page - 1) * limit

  txns = (
    db.query(Transaction)
      .order_by(Transaction.id)
      .offset(offset)
      .limit(limit)
      .all()
  )

  base_url = str(request.url).split('?')[0]
  def make_url(p: int) -> str:
    return str(request.url.include_query_params(page=p, limit=limit))

  next_page = make_url(page + 1) if len(txns) == limit else None
  prev_page = make_url(page - 1) if page > 1 else None

  if not txns and page != 1:
    raise HTTPException(status_code=404, detail="Page out of range")

  return PaginationResponse(
    page=page,
    limit=limit,
    count=len(txns),
    next_page=next_page,
    prev_page=prev_page,
    data=txns
  )
  
@api.get(
  "/users/",
  response_model=PaginationResponse[UserSchema],
  summary="List users with pagination"
)
def get_users(
  request: Request,
  page: int = Query(1, ge=1, description="Page number (starting from 1)"),
  limit: int = Query(50, ge=1, le=1000, description="Page size"),
  db: Session = Depends(get_db)
):
  offset = (page - 1) * limit

  users = (
    db.query(User)
      .order_by(User.id)
      .offset(offset)
      .limit(limit)
      .all()
  )

  base_url = str(request.url).split('?')[0]
  def make_url(p: int) -> str:
    return str(request.url.include_query_params(page=p, limit=limit))

  next_page = make_url(page + 1) if len(users) == limit else None
  prev_page = make_url(page - 1) if page > 1 else None

  if not users and page != 1:
    raise HTTPException(status_code=404, detail="Page out of range")

  return PaginationResponse(
    page=page,
    limit=limit,
    count=len(users),
    next_page=next_page,
    prev_page=prev_page,
    data=users
  )
  
@api.get(
  "/users/{user_id}/transactions",
  response_model=List[TransactionSchema],
  summary="Get all transactions for a given user"
)
def get_user_transactions(
    user_id: int,
    db: Session = Depends(get_db)
):
  txns = (
    db.query(Transaction)
      .filter(Transaction.client_id == user_id)
      .order_by(Transaction.id)
      .limit(10)
      .all()
  )
  if not txns:
    raise HTTPException(status_code=404, detail="No transactions found for this user")
  return txns
  
  
app.include_router(api)