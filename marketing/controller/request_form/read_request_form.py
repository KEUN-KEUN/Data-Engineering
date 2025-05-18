from pydantic import BaseModel

class ReadRequestForm(BaseModel):
    customer_id: int
