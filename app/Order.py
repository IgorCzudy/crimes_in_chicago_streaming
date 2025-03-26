from dataclasses import dataclass, field
from enum import Enum
import uuid

class Status(Enum):
    PENDING = 1 
    PROCESSING = 2 
    SHIPPED = 3 
    DELIVERED = 4 
    CANCELLED = 5
    
    def __str__(self):
        return self.name


item_name_to_uuid = {}

@dataclass
class Item():
    name: str
    quantity: int
    price_per_unit: float
    item_id: str = field(init=False)

    def __post_init__(self):
        if self.name not in item_name_to_uuid.keys():
            item_name_to_uuid[self.name] = uuid.uuid4()
        
        self.item_id = str(item_name_to_uuid[self.name])


@dataclass
class Order:
    customer_name: str
    items: list[Item]
    status: Status
    total_price: float = field(init=False)  # Don't provide it in the constructor
    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))


    def __post_init__(self):
        self.total_price = sum([item.price_per_unit * item.quantity for item in self.items])

from datetime import datetime

@dataclass
class StatusUpdateEvent:
    order_id: str
    new_status: Status
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
