# -*- coding: utf-8 -*-

import pprint
def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    """ each ofthe following pipelines answers a specific question"""

    # What are the top 10 amenities?
    pipeline = [{'$match':{'created.user':{'$exists':True}}},
                {'$group':{'_id':'$created.user',
                           'count':{'$sum':1}}},
                {'$sort':{'count':-1}},
                {'$limit':10}
               ]

    # What type of restaurants are the most popular?
    pipeline2 = [{'$match':{'cuisine':{'$exists':True}}},
                {'$group':{'_id':'$cuisine',
                           'count':{'$sum':1}}},
                {'$sort':{'count':-1}},
                {'$limit':10}
             ]
    
    # Who are the most prolific users?
    pipeline3 = [{'$match':{'created.user':{'$exists':True}}},
                {'$group':{'_id':'$created.user',
                           'count':{'$sum':1}}},
                {'$sort':{'count':-1}},
                {'$limit':10}
               ]

    # subsitute different pipeline versions depending on the question being asked
    return pipeline

db = get_db('osm_waterloo')
pipeline = make_pipeline()
result = db.nodes_and_ways.aggregate(pipeline)

for r in result:
    pprint.pprint(r)