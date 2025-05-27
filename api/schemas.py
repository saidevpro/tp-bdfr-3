from pydantic import BaseModel
from datetime import date
from typing import Generic, TypeVar, List, Optional
from pydantic.generics import GenericModel
from pydantic import BaseModel, ConfigDict


class UserSchema(BaseModel):
  model_config = ConfigDict(from_attributes=True) 
  
  id: int
  gender: str
  age: int
  retirement_age: int
  address: str
  credit_score: int
  

class TransactionSchema(BaseModel):
  model_config = ConfigDict(from_attributes=True) 
  
  id: int
  date: date
  client_id: int
  amount: float
  merchant_city: str
  

class TransactionFullSchema(BaseModel):
  model_config = ConfigDict(from_attributes=True) 
  
  id: int
  date: date
  client_id: int
  amount: float
  merchant_city: str
  client: UserSchema
  

DataT = TypeVar("DataT")

class PaginationResponse(GenericModel, Generic[DataT]):
  page: int
  limit: int
  count: int
  next_page: Optional[str]
  prev_page: Optional[str]
  data: List[DataT]
  