from pydantic import BaseModel
from typing import Optional

from marketing.entity.campaign_type import CampaignType
from marketing.entity.gender import Gender
from marketing.entity.user_response_type import UserResponseType


class UpdateRequestForm(BaseModel):
    customer_id: int
    age: Optional[int] = None
    gender: Optional[Gender] = None
    campaign_type: Optional[CampaignType] = None
    user_response: Optional[UserResponseType] = None
