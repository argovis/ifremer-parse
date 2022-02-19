# usage: python establishCollection.py
# creates an empty, unindexed collection in the argo db with schema validation enforcement

from pymongo import MongoClient
import sys

client = MongoClient('mongodb://database/argo')
db = client.argo

db.profilesx.drop()
db.create_collection("profilesx")

profileSchema = {"$jsonSchema":{
  "bsonType": "object",
  "required": ["_id", "basin", "data_type", "geolocation", "source", "timestamp", "date_updated_argovis", "date_updated_source", "cycle_number", "platform_wmo_number"],
  "properties": {
    "_id": {
        "bsonType": "string"
    },
    "basin": {
        "bsonType": "int"
    },
    "data_type": {
        "bsonType": "string"
    },
    "doi": {
        "bsonType": "string"
    },
    "geolocation": {
        "bsonType": "object",
        "required": ["type", "coordinates"],
        "properties": {
            "type":{
                "enum": ["Point"]
            },
            "coordinates":{
                "bsonType": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": {
                    "bsonType": ["double", "int"]
                }
            }
        }
    },
    "instrument": {
        "bsonType": "string"
    },
    "data": {
        "bsonType": "array",
        "items": {
            "bsonType": "array"
        }
    },
    "data_keys": {
        "bsonType": "array",
        "items": {
            "bsonType": "string"
        }
    },
    "data_keys_source": {
        "bsonType": "array",
        "items": {
            "bsonType": "string"
        }
    },
    "source": {
        "bsonType": "array",
        "items": {
            "bsonType": "string"
        }
    },
    "source_url": {
        "bsonType": "array",
        "items": {
            "bsonType": "string"
        }
    },
    "timestamp": {
        "bsonType": "date"
    },
    "date_updated_argovis": {
        "bsonType": "date"
    },
    "date_updated_source": {
        "bsonType": "array",
        "items": {
            "bsonType": "date"
        }
    },
    "pi_name": {
        "bsonType": "string",
    },
    "country": {
        "bsonType": "string"
    },
    "data_center": {
        "bsonType": "string"
    },
    "profile_direction": {
        "bsonType": "string"
    },
    # argo-specific below this line
    "geolocation_argoqc": {
        "bsonType": "int"
    },
    "timestamp_argoqc": {
        "bsonType": "int"
    },
    "cycle_number": {
        "bsonType": "int"
    },
    "fleetmonitoring": {
        "bsonType": "string"
    },
    "oceanops": {
        "bsonType": "string"
    },
    "data_keys_mode": {
        "bsonType": "array",
        "items": {
            "bsonType": "string"
        }
    },
    "platform_wmo_number": {
        "bsonType": "int"
    },
    "platform_type": {
        "bsonType": "string"
    },
    "positioning_system": {
        "bsonType": "string"
    },
    "vertical_sampling_scheme": {
        "bsonType": "string"
    },
    "wmo_inst_type": {
        "bsonType": "string"
    }
  },
  "dependencies": {
    "data": ["data_keys"]
  }
}}

db.command('collMod','profilesx', validator=profileSchema, validationLevel='strict')