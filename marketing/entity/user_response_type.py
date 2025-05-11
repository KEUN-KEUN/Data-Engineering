from enum import Enum
from typing import Union

class UserResponseType(str, Enum):
    ignored = 'Ignored'
    clicked = 'Clicked'
    purchased = 'Purchased'

    @staticmethod
    def from_int(value: Union[int, str]) -> 'ResponseType':
        mapping = {
            0: UserResponseType.ignored,
            1: UserResponseType.clicked,
            2: UserResponseType.purchased,
            '0': UserResponseType.ignored,
            '1': UserResponseType.clicked,
            '2': UserResponseType.purchased,
        }
        return mapping.get(value, UserResponseType.ignored)