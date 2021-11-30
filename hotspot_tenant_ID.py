from pymongo import MongoClient
from pprint import pprint

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient(
    "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
)
result = client["crm"]["hotspot"].aggregate(
    [
        {"$match": {"customer_id": "61a659c038911dabe4b3fa11"}},
        {
            "$lookup": {
                "from": "wifi",
                "localField": "data.wifiId",
                "foreignField": "_id",
                "as": "register_tenant_id",
            }
        },
        {"$unwind": {"path": "$register_tenant_id"}},
        {
            "$lookup": {
                "from": "site",
                "localField": "register_tenant_id.siteId",
                "foreignField": "_id",
                "as": "register_site_id",
            }
        },
        {"$unwind": {"path": "$register_site_id"}},
        {
            "$project": {
                "customer_id": 1,
                "customer_identifier": 1,
                "register_site_id.tenantId": 1,
            }
        },
    ]
)
