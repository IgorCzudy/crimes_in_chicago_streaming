from pydantic import BaseModel
from datetime import datetime
from typing import Optional



class CrimeRecord(BaseModel):
    ID: int
    Case_Number: str
    Date: datetime
    Block: str
    IUCR: str
    Primary_Type: str
    Description: str
    Location_Description: Optional[str]
    Arrest: bool
    Domestic: bool
    Beat: int
    District: Optional[int]
    Ward: Optional[int]
    Community_Area: Optional[int]
    FBI_Code: str
    X_Coordinate: Optional[int]
    Y_Coordinate: Optional[int]
    Year: int
    Updated_On: datetime
    Latitude: Optional[float]
    Longitude: Optional[float]
    Location: Optional[str]

    class Config:
        arbitrary_types_allowed = True
