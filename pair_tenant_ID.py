from pymongo import MongoClient
from pprint import pprint

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient(
    "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
)
result = client["crm"]["cast_pair"].aggregate(
    [
        {"$match": {"customer_id": "61a65a35357eef2901b41259"}},
        {
            "$lookup": {
                "from": "castCore",
                "localField": "data.kkResource",
                "foreignField": "kkResource",
                "as": "pair_castcore_kk",
            }
        },
        {"$unwind": {"path": "$pair_castcore_kk"}},
        {
            "$lookup": {
                "from": "site",
                "localField": "pair_castcore_kk.siteId",
                "foreignField": "_id",
                "as": "pair_site",
            }
        },
        {"$unwind": {"path": "$pair_site"}},
        {
            "$project": {
                "customer_id": 1,
                "customer_identifier": 1,
                "pair_site.tenantId": 1,
            }
        },
    ]
)
