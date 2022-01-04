import locale

from datetime import datetime
from pydantic import BaseModel, validator, ValidationError
from typing import Optional

locale.setlocale(locale.LC_ALL, "es_ES")  # To parse correctly the months


class Game(BaseModel):
    datetime: datetime
    home_team: str
    away_team: str
    home_goal: Optional[int]
    away_goal: Optional[int]
    matchday: int

    @validator("matchday", pre=True)
    def validate_matchday(cls, value):
        if value is None:
            raise ValidationError("matchday is required")

        if isinstance(value, int):
            # For loading from .txt file
            return value

        return value.strip(" ")[-1]

    @validator("datetime", pre=True)
    def validate_datetime(cls, value):
        if value is None:
            raise ValidationError("datetime is required")
        try:
            return datetime.strptime(value, "%d %b %y a las %H:%M")
        except Exception as e:
            # For loading from .txt file
            return datetime.strptime(value, "%Y-%m-%d %H:%M")
