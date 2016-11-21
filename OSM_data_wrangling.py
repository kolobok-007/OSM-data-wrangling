# -*- coding: utf-8 -*-
"""
Created on Wed Jun 08 12:57:23 2016

@author: Mikhail
"""

import xml.etree.cElementTree as ET
import pprint
import re
from collections import defaultdict
import codecs
import json
from pymongo import MongoClient
    
#import numpy as np

#global variables
osmfile='waterloo-region_canada.osm'
expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons", "Way","Walk","Terrace","Path","Line", "Hollow",
            "Gate","Crescent","Close","Circle", "Ridge","Run","Park","Strasse", "Baseline",
            "Boardwalk", "Cove","Crestway","Crossing"]

directions = ['East','West','North','South']

directions_mapping= {'S':'South',
                     'N':'North',
                     'W':'West',
                     'E':'East'
                    }

reverse = ['Concession Road','Sideroad','Highway','County Road']

mapping = { "St": "Street",
            "St.": "Street",
            'Steet':'Street',
            'Ave':'Avenue',
            'Rd.':'Road',
            'Rd':'Road',
            'road':'Road',
            'AVenue':'Avenue',
            'Cresent':'Crescent',
            'Dr':'Drive',
            'Dr.':'Drive'          
            }

#global re expressions
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
l=r'[ABCDEFGHIJKLMNOPQRSTUVWXYZ]'
postalcode_re = re.compile(l+r'\d'+l+' '+r'\d'+l+r'\d')
reverse_re=re.compile(r'\d+ ')

#Audit functions
def key_type(element, keys):
    # Function to count type of keys
    if element.tag == "tag":
        value = element.get('k')
        if lower.search(value)!=None:
            keys['lower']+=1
        elif lower_colon.search(value)!=None:
            keys['lower_colon']+=1
        elif problemchars.search(value)!=None:
            keys['problemchars']+=1
        else:
            keys['other']+=1
    return keys

def get_user(element):
    user = element.get('user')
    return user

def audit_street_type(street_types, street_name):
    # Function to look for unexpected street types
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected and street_type not in directions:
             street_types[street_type].add(street_name)

def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def is_postal_name(elem):
    return (elem.attrib['k'] == "addr:postcode")
    
def audit(filename):
    # Function that combines different audit types
    # Variables for different types audits
    k_types = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    tag_count = defaultdict(int)
    users = defaultdict(int)
    street_types = defaultdict(set)
    postal_codes=defaultdict(int)    
    
    for ev, element in ET.iterparse(filename):
        k_types = key_type(element, k_types)
        tag_count[element.tag]+=1
        users[get_user(element)]+=1
        
        #Aduit tags
        if element.tag == "node" or element.tag == "way":
            for tag in element.iter("tag"):
                value=tag.attrib['v']
                #Audit street types                
                if is_street_name(tag):
                    audit_street_type(street_types, value)
                #Audit postal codes
                elif is_postal_name(tag):
                    if postalcode_re.search(value)==None:
                        postal_codes[value]+=1

    return tag_count, k_types, users, street_types, postal_codes

#Fixing functions
def fix_street_name(name):
    # Function to fix street names
    st_name = street_type_re.search(name)
    rev_m = reverse_re.search(name)     
    
    if st_name.group() in expected:
        return name
    elif rev_m and name[rev_m.end():] in reverse: #if the name and number should be reversed
        return name[rev_m.end():] + rev_m.group()
    elif st_name.group() in directions_mapping.keys(): #Wrong abreviation of the direction
        #Feed it back into the function for further processing
        name = name[:st_name.start()]+directions_mapping[st_name.group()]
        return fix_street_name(name)
    elif st_name.group() in directions:
        name = name[:st_name.start()].strip()
        #Feed it back into the function and then append direction to it
        return fix_street_name(name) + ' ' + st_name.group()
    elif st_name.group() in mapping.keys():
        return name[:st_name.start()]+mapping[st_name.group()]
    else:
        return name

def fix_postal_code(code):
    #This will fix two problems with postal code: not all uppercase and no space    
    code_m = postalcode_re.search(code)
    if code_m==None:
        if len(code)==7:
            return code.upper()
        elif len(code)==6:
            return (code[:3]+' ' + code[3:]).upper()
        else:
            return
# Final output functions
def shape_element(element):
    # Function to process an element
    node = {}
    if element.tag == "node" or element.tag == "way" :
        node['id']=element.get('id')
        node['type']=element.tag
        node['visible']=element.get('visible')
        created={}
        created['version']=element.get('version')
        created['changeset']=element.get('changeset')
        created['timestamp']=element.get('timestamp')
        created['user']=element.get('user')
        created['uid']=element.get('uid')
        node['created']=created
        
        lat=element.get('lat')
        lon=element.get('lon')
        if lat == None or lon == None:
            lat=None
            lon=None
        else:
            lat = float(lat)
            lon = float(lon)
        node['pos']=[lat, lon]
        address={}
        node_refs=[]
        
        #Loop through tags and node references
        for tag in element:
            k = tag.get('k')
            v = tag.get('v')
            ref = tag.get('ref')
            
            if ref:
                node_refs.append(ref)
                
            if k == 'addr:housenumber':
                address['housenumber']=v
            elif k == 'addr:street':
                address['street']= fix_street_name(v) #fix street name
            elif k == 'addr:postcode':
                address['postcode']= fix_postal_code(v) #fix postal code
            elif k == 'amenity':
                node['amenity']=v
            elif k == 'cuisine':
                node['cuisine']= v
            elif k == 'name':
                node['name']= v
            elif k == 'phone':
                node['phone']=v
            
            if element.tag=='way':
                node['node_refs'] = node_refs
                
            if address!={}:
                node['address']=address
        return node
    else:
        return None

def process_map(file_in, pretty = False):
    # Function to process the .OSM file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def get_db(db_name):
    # Function to connect to mongodb client
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

if __name__=='__main__':
    ##############################################################################
    # Auding done first, iteratively
    # count_of_tags, k_types, users, bad_street_types, bad_postal_codes =  audit(osmfile)
    # print k_types 
    # print count_of_tags
    # print len(users)
    # pprint.pprint(dict(bad_street_types))
    # pprint.pprint(dict(bad_postal_codes))

    ####################################################################################
    # Process the data and insert it into mongoDB
    data = process_map(osmfile,pretty=True)
    db = get_db('osm_waterloo')
    db.nodes_and_ways.insert_many(data)
