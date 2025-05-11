from enum import Enum
from typing import Union

class CampaignType(str, Enum):
    email = 'Email'
    sms = 'SMS'
    push = 'Push'

    @staticmethod
    def from_int(value: Union[int, str]) -> 'CampaignType':
        mapping = {
            0: CampaignType.email,
            1: CampaignType.sms,
            2: CampaignType.push,
            '0': CampaignType.email,
            '1': CampaignType.sms,
            '2': CampaignType.push,
        }
        return mapping.get(value, CampaignType.email)