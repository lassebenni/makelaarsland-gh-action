from datetime import datetime, date
import re
from typing import Optional
from pydantic import BaseModel, validator, Field


class HouseListing(BaseModel):
    date: str = Field(default_factory=date.today)
    datetime: str = Field(default_factory=datetime.now)
    vraagprijs: Optional[str] = ""
    city: Optional[str] = ""
    streetname: Optional[str] = ""
    status: Optional[str] = ""
    aanvaarding: Optional[str] = ""
    soort_woonhuis: Optional[str] = ""
    soort_bouw: Optional[str] = ""
    bouwjaar: Optional[str] = ""
    soort_dak: Optional[str] = ""
    kadastrale_gegevens: Optional[str] = ""
    woonoppervlakte_m2: Optional[str] = Field(default="", alias="woonoppervlakte")
    externe_bergruimte: Optional[str] = ""
    overig_inpandige_ruimte: Optional[str] = ""
    perceeloppervlakte: Optional[str] = ""
    inhoud: Optional[str] = ""
    aantal_kamers: Optional[str] = ""
    aantal_badkamers: Optional[str] = ""
    badkamervoorzieningen: Optional[str] = ""
    voorzieningen: Optional[str] = ""
    energielabel: Optional[str] = ""
    isolatie: Optional[str] = ""
    verwarming: Optional[str] = ""
    warm_water: Optional[str] = ""

    class Config:
        orm_mode = True

    @validator("vraagprijs")
    def clean_vraagprijs(cls, value):
        if value:
            value = re.sub(r"[^\d.]", "", value)
        return value
