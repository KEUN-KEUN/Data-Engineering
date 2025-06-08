from pydantic import BaseModel


class KafkaEndpointRequestForm(BaseModel):
    message: str