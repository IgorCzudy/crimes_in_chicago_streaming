from confluent_kafka import Producer
from Order import Order, Item, Status, StatusUpdateEvent
import json
import dataclasses
from faker import Faker
import random
import time

def order_to_dict(order):
    order_dict = dataclasses.asdict(order)
    order_dict['status'] = str(order_dict['status'] )
    return order_dict

def statusUpdateEvent_to_dict(statusUpdateEvent):
    statusUpdateEvent_dic = dataclasses.asdict(statusUpdateEvent)
    statusUpdateEvent_dic['new_status'] = str(statusUpdateEvent_dic['new_status'] )
    return statusUpdateEvent_dic



def deliver_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


p = Producer({'bootstrap.servers': 'localhost:9092'})
faker = Faker()

random.seed(42)
items = ["Laptop", "Mouse", "Keyboard", "Monitor", "USB-C Hub", "Webcam", "Office Chair"]
orders_in_system = {}

while True:
    name = faker.name()
    
    order = Order(customer_name=name,
            items=[
                    Item(name=random.choice(items), quantity=random.randint(1,5), price_per_unit=random.randint(1000,1500)),
                    Item(name=random.choice(items), quantity=random.randint(1,5), price_per_unit=random.randint(1000,1500)),
                ],
            status=Status.PENDING
        )
    orders_in_system[order.order_id] = order

    order_dict = order_to_dict(order)
    order_json = json.dumps(order_dict)
    p.produce('orders.created',
              key = order.order_id,
              value = order_json.encode('utf-8'), 
              callback=deliver_report)
    

    if random.random() < 0.1: #10% massege update sended 
        current_order = random.choice(list(orders_in_system.values()))


        transitions = {
            Status.PENDING: [Status.PROCESSING, Status.CANCELLED],
            Status.PROCESSING: [Status.SHIPPED, Status.CANCELLED],
            Status.SHIPPED: [Status.DELIVERED],
            Status.DELIVERED: [],
            Status.CANCELLED: []
        }
        new_status = random.choice(transitions[current_order.status])

        if new_status == Status.CANCELLED or new_status == Status.DELIVERED:
            del orders_in_system[current_order.order_id]

        status_update_event = StatusUpdateEvent(order_id=current_order.order_id, new_status=new_status)
                
        print(f"Updating order {current_order.order_id}")

        p.produce('orders.status_updates',
                  key = current_order.order_id,
                  value = json.dumps(statusUpdateEvent_to_dict(status_update_event)).encode('utf-8'), 
                  callback=deliver_report)

    p.poll(0)
    p.flush()
    time.sleep(1)

        