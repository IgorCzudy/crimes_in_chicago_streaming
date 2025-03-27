from confluent_kafka import Consumer
import json
from Order import Order, StatusUpdateEvent
import pymongo
from fastapi.encoders import jsonable_encoder


db_client = pymongo.MongoClient("mongodb://localhost:27017/")
db_client.drop_database("ordersDB")
db = db_client["ordersDB"]
colection = db["orders"]

def main():
    c = Consumer({
        'bootstrap.servers': "localhost:9092",
        'group.id': 'orders-group',
        'auto.offset.reset': 'earliest'
    })

    c.subscribe(["orders.created", "orders.status_updates"])

    while True:
        msg = c.poll(0.1)

        if msg is None:
            continue

        elif msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        elif msg.topic() == "orders.created":
            order = Order.model_validate(json.loads(msg.value().decode('utf-8')))
            print(order)
            order = jsonable_encoder(order)
            colection.insert_one(order)


        elif msg.topic() == "orders.status_updates":
            statusUpdateEvent = StatusUpdateEvent.model_validate(json.loads(msg.value().decode('utf-8')))
            print(statusUpdateEvent)
            statusUpdateEvent_dic = statusUpdateEvent.model_dump()
            new_values = {"$set": {"status": str(statusUpdateEvent_dic["new_status"])}}
            colection.update_one({"_id": statusUpdateEvent_dic["order_id"]},
                                 new_values)

    c.close()

        
if __name__=="__main__":
    main()
