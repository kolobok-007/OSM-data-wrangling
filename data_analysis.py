# -*- coding: utf-8 -*-
"""
Created on Thu Jun 09 16:20:58 2016

@author: Mikhail
"""
import pprint
def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def make_pipeline():
    # complete the aggregation pipeline
    pipeline = [{'$match':{'created.user':{'$exists':True}}},
                {'$group':{'_id':'$created.user',
                           'count':{'$sum':1}}},
                {'$sort':{'count':-1}},
                {'$limit':10}
               ]
    return pipeline

db = get_db('osm_waterloo')
pipeline = make_pipeline()
result = db.nodes_and_ways.aggregate(pipeline)

for r in result:
    pprint.pprint(r)