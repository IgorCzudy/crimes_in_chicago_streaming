from enum import Enum
import uuid
from pydantic import BaseModel, model_validator, Field
from datetime import datetime


class Status(Enum):
    PENDING = 1 
    PROCESSING = 2 
    SHIPPED = 3 
    DELIVERED = 4 
    CANCELLED = 5
    
    def __str__(self):
        return str(self.name)


item_name_to_uuid = {}
class Item(BaseModel):
    name: str
    quantity: int
    price_per_unit: float
    item_id: str = Field(init=False)

    @model_validator(mode="before")
    def set_iteam_id(cls, values):
        name = values.get("name")
        if name:
            if name not in item_name_to_uuid.keys():
                item_name_to_uuid[name] = uuid.uuid4()
        
            values["item_id"] = str(item_name_to_uuid[name])
        return values
    



class Order(BaseModel):
    customer_name: str
    items: list[Item]
    status: Status
    order_id: str = Field(default_factory=lambda: str(uuid.uuid4()), alias="_id")
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    # class Config:
    #     json_encoders = {
    #         Status: lambda v: str(v)
    #     }


class StatusUpdateEvent(BaseModel):
    order_id: str
    new_status: Status
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    # class Config:
    #     json_encoders = {
    #         Status: lambda v: str(v)
    #     }
