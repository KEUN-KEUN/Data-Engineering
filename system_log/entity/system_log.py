from pydantic import BaseModel, field_validator
from datetime import datetime


class SystemLog(BaseModel):
    timestamp: datetime
    user_id: str
    action: str
    duration_ms: int

    @field_validator("duration_ms")
    @classmethod
    def validate_duration(cls, v):
        if v < 0:
            raise ValueError("duration_ms must be non-negative")
        return v
