from pydantic import BaseModel


class PhoneInfo(BaseModel):
    inn: int
    operator: str
    region: str
    sub_region: str
