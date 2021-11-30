from pymongo import MongoClient
from pprint import pprint

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient(
    "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
)
result = client["crm"]["cast_playback"].aggregate(
    [
        {"$match": {"customer_id": "61a65a34357eef2901b4122a"}},
        {
            "$lookup": {
                "from": "castCore",
                "localField": "data.kkResource",
                "foreignField": "kkResource",
                "as": "playback_castcore_kk",
            }
        },
        {"$unwind": {"path": "$playback_castcore_kk"}},
        {
            "$lookup": {
                "from": "site",
                "localField": "playback_castcore_kk.siteId",
                "foreignField": "_id",
                "as": "playback_site",
            }
        },
        {"$unwind": {"path": "$playback_site"}},
        {"$project": {"customer_id": 1, "playback_site.tenantId": 1}},
    ]
)
