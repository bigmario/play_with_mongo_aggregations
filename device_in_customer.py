from pymongo import MongoClient
from pprint import pprint

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient(
    "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
)

result = client["crm"]["customer"].aggregate(
    [
        {"$match": {"_id": "61a5182e8ff68926b355a51e"}},
        {
            "$addFields": {
                "device_in_customer": {
                    "$in": ["6012b8043b1b2933559c08e9", "$device_ids"]
                }
            }
        },
        {"$project": {"_id": 1, "device_in_customer": 1}},
    ]
)

lista = [doc for doc in result]
pprint(lista)
