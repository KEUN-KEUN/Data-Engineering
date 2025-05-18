from pydantic import BaseModel

class RemoveRequestForm(BaseModel):
    customer_id: int
