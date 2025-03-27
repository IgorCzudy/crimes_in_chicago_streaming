from fastapi import FastAPI, Request, HTTPException, status
from pymongo import MongoClient
from typing import List
from Order import Order

app = FastAPI()

@app.on_event("startup")
def startup_db_client():
    app.mongodb_client = MongoClient("mongodb://localhost:27017/")
    app.database = app.mongodb_client["ordersDB"]
    print("Connected to the MongoDB")


@app.on_event("shutdown")
def shutdown_db_client():
    app.mongodb_client.close()


@app.get("/", response_description="Listo of all orders", response_model=List[Order])
def list_orders(request: Request):
    orders = list(request.app.database["orders"].find(limit=100))

    return orders

@app.get("/{customer_name}", response_description="Get orders of cerin customer", response_model=List[Order])
def find_orders(customer_name: str, request: Request):
    orders = request.app.database["orders"].find({"customer_name": customer_name})
    if orders:
        return list(orders)
    else:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Orders of customer_name: {customer_name} not found")

