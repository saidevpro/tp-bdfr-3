from sqlalchemy import Column, Integer, String, Date, Float, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from database import Base

class Transaction(Base):
  __tablename__ = "transactions"
  id              = Column(Integer, primary_key=True, index=True)
  date            = Column(Date)
  client_id       = Column(Integer, ForeignKey("users.id"), nullable=False)
  card_id         = Column(Integer)
  amount          = Column(Integer)
  use_chip        = Column(String)
  merchant_id     = Column(Integer)
  merchant_city   = Column(String)
  merchant_state  = Column(String)
  zip             = Column(String)
  mcc             = Column(Integer)
  currency        = Column(String)
  has_fraud       = Column(String)
  
  client = relationship("User", back_populates="transactions")


class User(Base):
  __tablename__ = "users"
  id                    = Column(Integer, primary_key=True, index=True)
  age                   = Column("current_age", Integer)
  retirement_age        = Column(Integer)
  birth_year            = Column(Integer)
  birth_month           = Column(Integer)
  gender                = Column(String)
  address               = Column(String)
  per_capita_income     = Column(Float)
  yearly_income         = Column(Float)
  total_debt            = Column(Float)
  credit_score          = Column(Integer)
  num_credit_cards      = Column(Integer)
  currency              = Column(String)
  
  transactions = relationship("Transaction", back_populates="client")

