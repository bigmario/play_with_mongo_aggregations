from datetime import datetime
from pprint import pprint

from pymongo import MongoClient

# client = MongoClient(
#     "mongodb+srv://mario:14567498@hotspot.s19tf.mongodb.net/hotspot?retryWrites=true&w=majority"
# )

client = MongoClient(
    "mongodb+srv://admin:admin@hotspot.tnhog.mongodb.net/hotspot?retryWrites=true&w=majority"
)

db = client.hotspot

pair_collection = db.pair

# result = pair_collection.aggregate(
#     [
#         {"$match": {}},
#         {
#             "$lookup": {
#                 "from": "playback",
#                 "localField": "deviceId",
#                 "foreignField": "deviceIds",
#                 "as": "playback_pair",
#             }
#         },
#         {
#             "$unwind": "$playback_pair",
#         },
#     ]
# )

result_2 = pair_collection.aggregate(
    [
        {"$match": {}},
        {
            "$lookup": {
                "from": "playback",
                "localField": "deviceId",
                "foreignField": "deviceIds",
                "as": "playback_pair",
            },
        },
        {
            "$unwind": "$playback_pair",
        },
        {
            "$match": {
                "startDate": {"$exists": True},
                "endDate": {"$exists": True},
            }
        },
        {
            "$addFields": {
                "start": {
                    "$dateToString": {
                        "date": "$startDate",
                        "format": "%Y-%m-%dT%H:%M:%S.%L%z",
                        "timezone": "+00:00",
                    }
                },
                "end": {
                    "$dateToString": {
                        "date": "$endDate",
                        "format": "%Y-%m-%dT%H:%M:%S.%L%z",
                        "timezone": "+00:00",
                    }
                },
            }
        },
        # {
        #     "$match": {
        #         "playback_pair.startDate": {
        #             "$gte": "$startDate",
        #             "$lt": "$endDate",
        #         }
        #     }
        # },
        # {
        #     "$match": {
        #         "playback_pair.startDate": {
        #             "$gte": 'ISODate("2021-07-20T16:04:37.782+0000")',
        #             "$lt": 'ISODate("2021-07-21T15:35:39.226+0000")',
        #         }
        #     }
        # },
        {"$addFields": {"device_Id": {"$toObjectId": "$deviceId"}}},
        {
            "$lookup": {
                "from": "device",
                "localField": "device_Id",
                "foreignField": "_id",
                "as": "device_pair",
            }
        },
    ]
)

# result_3 = pair_collection.aggregate([{"$facet": {}}])

try:
    lista = (doc for doc in result_2)
    pprint(next(lista))
except Exception as e:
    print(e)
