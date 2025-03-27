from confluent_kafka import Producer
from Order import Order, Item, Status, StatusUpdateEvent
from faker import Faker
import random
import time


faker = Faker()
item_names = ["Laptop", "Mouse", "Keyboard", "Monitor", "USB-C Hub", "Webcam", "Office Chair"]
orders_in_system = {}


def deliver_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def create_random_order():
    name = faker.name()
    items = []
    for _ in range(random.randint(1,4)):
        items.append(Item(name=random.choice(item_names), quantity=random.randint(1,5), price_per_unit=random.randint(1000,1500)))

    order = Order(customer_name=name,
            items=items,
            status=Status.PENDING
        )
    orders_in_system[order.order_id] = order

    return order

def update_order_status():
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

    return StatusUpdateEvent(order_id=current_order.order_id, new_status=new_status)


def main():
    p = Producer({'bootstrap.servers': 'localhost:9092'})
    random.seed(42)
    
    while True:

        order = create_random_order()
        order_json = order.model_dump_json()
    
        p.produce('orders.created',
                key = order.order_id,
                value = order_json.encode('utf-8'), 
                callback=deliver_report)
        p.poll(0)
        

        if random.random() < 0.1: #10% massege update sended             
            statusUpdateEvent = update_order_status()

            statusUpdateEvent_json = statusUpdateEvent.model_dump_json()

            p.produce('orders.status_updates',
                    key = statusUpdateEvent.order_id,
                    value = statusUpdateEvent_json.encode('utf-8'), 
                    callback=deliver_report)
            p.poll(0)
    
    
        time.sleep(1)

    p.flush()

        
if __name__=="__main__":
    main()
