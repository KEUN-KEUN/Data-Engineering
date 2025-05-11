from pydantic import BaseModel, field_validator

from marketing.entity.campaign_type import CampaignType
from marketing.entity.gender import Gender
from marketing.entity.user_response_type import UserResponseType


class MarketingData(BaseModel):
    customer_id: int
    age: int
    gender: Gender
    campaign_type: CampaignType
    user_response: UserResponseType

    @field_validator('campaign_type', mode='before')
    @classmethod
    def parse_campaign_type(cls, v):
        if isinstance(v, (int, str)) and str(v).isdigit():
            return CampaignType.from_int(v)
        return CampaignType(v.capitalize())

    @field_validator('user_response', mode='before')
    @classmethod
    def parse_response(cls, v):
        if isinstance(v, (int, str)) and str(v).isdigit():
            return UserResponseType.from_int(v)
        return UserResponseType(v.capitalize())

    @field_validator('gender', mode='before')
    @classmethod
    def parse_gender(cls, v):
        if isinstance(v, (int, str)) and str(v).isdigit():
            return Gender.from_int(v)
        return Gender(v.upper())
