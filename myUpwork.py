import upwork, os, json, time, glob
import pandas as pd
import numpy as np
import sqlite3
from pprint import pprint


"""
This script allows to:
1. Collect upwork data: both user profiles (provider_v2) and detailed profile (provider)
2. Insert Collected data in a sqlite database
3. Push data to sqlite3 database
"""
def initClient(public_key, secret_key):
    client = upwork.Client(public_key, secret_key)
    verifier = raw_input(
        'Please enter the verification code you get '
        'following this link:\n{0}\n\n> '.format(
            client.auth.get_authorize_url()))
    access_tkn = client.auth.get_access_token(verifier)
    client = upwork.Client(public_key, secret_key,
                           access_tkn[0], access_tkn[1])
    return client

def saveJson(data, filename):
    with open(filename,'w') as f:
        json.dump(data,f)

def loadJson(filename):
    with open(filename,'r') as f:
        j = json.load(f)
    return j


def collectCategories():
    raw = client.provider_v2.get_categories_metadata()
    categories = []
    try:
        for item in raw:
            for topic in item['topics']:
                categories.append((item['title'],topic['title']))
    except:
        print "some went bad"
    return categories


def collectProfiles(client, titles, directory, page_size = 100):
    """
    client: upwork client object
    titles: list of categories
    directory: directory where to dump collected profiles
    """
    pagesize = page_size
    
    skipped = []
    cont = True
    
    for title in titles:
        k = 0
        l = 100;
        while l >= pagesize:
            try:
                print title, k
                data = client.provider_v2.search_providers(data = {'title':title},
                                                           page_offset = k*pagesize,
                                                           page_size = pagesize)
                map(lambda x:saveJson(x,os.path.join(directory,'data/profiles/'+x['id']+'.json')),data)
                k+=1

            except:
                #in case of error, catch the title and append it to the list of skipped
                skipped.append(title)
            l = len(data)                   
    print "Profile collection complete"
    
    return skipped

def getProfileDetails(client, user_ids, directory,k=0):
    """
    input:
        client: upwork client object
        user_ids: list of strings
        directory: root directory of the project
        k: int, usser_ids list index to start with
    """
    skipped = []
    # introduce a count that help to move on when a certain user_id is not found
    # or if it is causing error
    n = 0
    while k < len(user_ids):
        try:
            profile = client.provider.get_provider(user_ids[k])
            saveJson(profile,directory+'/data/details/'+ids[k]+'.json')
            k+=1
            n = 0 #reset the count to zero if no error
        except:
            #start counting if an error is catched and skip that id
            #if it happens for more than 3 times
            n+=1 
            if n>=3:
                k += 1 #now skip the id and append it to the list to be returned
                skipped.append(user_ids[k])
                #dump the skipped id
                saveJson([],directory+'/data/tmp/'+ids[k]+'.json')
                print user_ids[k]

    return skipped
        

def createTable(cur, tableName, fields):
    cur.execute('DROP TABLE IF EXISTS '+ tableName)
    cur.execute('CREATE TABLE '+ tableName + fields)
    print 'table ', tableName, 'created.'

    
if __name__ == '__main__':
    directory = '/Users/cgirabawe/SideProjects/test'
    credentials = pd.read_csv(os.path.join(directory,'credentials.csv'))
    # client = initClient(credentials.key[0],credentials.secret[0])
    #1. collect categories
    # --> uncomment the following three lines to collect and save data on disk
    #print 'collecting categories...'
    #categories = collectCategories()
    #saveJson(categories,os.path.join(directory,'data/metadata/categories.json'))

    #2. collect profile data
    # --> uncomment the following three lines to collect and save data on disk
    #print 'collecting profiles ...'
    #data = collectProfiles(client, map(lambda x:x[1],categories), directory)
    
    #3. collect detailed data
    # --> uncomment the following three lines to collect and save data on disk
    # print 'collecting details on profiles'
    # ids = glob.glob(os.path.join(directory,'data/profiles/*.json'))
    # ids = map(lambda x:os.path.basename(x)[:-5],ids)
    # det = glob.glob(os.path.join(directory,'data/details/*.json'))
    # det = map(lambda x:os.path.basename(x)[:-5],det)
    # ids = list(set(ids)-set(det))
    # print "number of profiles to fetch:", len(ids)
    # skipped_profiles = getProfileDetails(client, ids, directory)
    
    #4. Build database
    conn = sqlite3.connect('upworkdb.db')
    cur = conn.cursor()
    # insert profile data collected above in the database
    # pass
