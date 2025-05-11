from enum import Enum
from typing import Union


class Gender(str, Enum):
    male = 'MALE'
    female = 'FEMALE'

    @staticmethod
    def from_int(value: Union[int, str]) -> 'Gender':
        mapping = {
            0: Gender.male,
            1: Gender.female,
            '0': Gender.male,
            '1': Gender.female,
        }
        return mapping.get(value, Gender.male)