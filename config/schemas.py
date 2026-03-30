from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class Location(BaseModel):
    '''Схема локации'''

    city: Optional[str] = None
    country: Optional[str] = None
    countryCode: Optional[str] = None
    countryName: Optional[str] = None


class Status(BaseModel):
    '''Схема статуса'''

    iconText: Optional[str] = None
    text: Optional[str] = None


class OKUsers(BaseModel):
    '''Основная схема пользователя OK'''

    uid: int
    birthday: Optional[str] = None
    byear: Optional[int] = None
    bmonth: Optional[int] = None
    bday: Optional[int] = None
    age: Optional[int] = None
    current_status: Optional[str] = None
    current_status_date: Optional[str] = None
    first_name: Optional[str] = None
    gender: Optional[str] = None
    last_name: Optional[str] = None
    last_online: Optional[str] = None
    locale: Optional[str] = None
    location: Optional[Location] = None
    name: Optional[str] = None
    online: Optional[str] = None
    premium: Optional[bool] = None
    private: Optional[bool] = None
    registered_date: Optional[str] = None
    shortname: Optional[str] = None
    status: Optional[Status] = None
    ts: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True


class ProcessedUser(OKUsers):
    '''Расширенная схема с обработанными полями'''

    ts: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
