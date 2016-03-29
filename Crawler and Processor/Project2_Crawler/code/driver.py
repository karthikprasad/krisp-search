
'''
@Author: {prasadkr,pmantrip,rishabas}@uci.edu
'''

import pymongo

def num_unique_pages(db):
    coll = db['web-index']
    print coll.count()

def subdomain_count(db, limit=None):
    coll = db['web-index']
    for p in coll.aggregate([{'$group':{'_id':'$subdomain', 'count':{'$sum':1 }}},{'$sort': {'_id': -1}}]):
        print str(p['_id']), ',', p['count']

def longest_page(db):
    coll = db['web-index']
    m = coll.find_one(sort=[('num_words', -1)])
    print m['url'], m['num_words']

def tokens_count(db, limit=500):
    coll = db['term-count']
    i = 0
    l = []
    for p in coll.aggregate([{'$group':{'_id':'$term', 'count':{'$sum':1 } } }, {'$sort':{'count':-1}}]):
        if i == limit:
            break
        l.append(str(p['_id']) + '\t' + str(p['count']) + '\n')
        i += 1
    with open('most_common_words.txt', 'w') as f:
        f.writelines(l)

def threegrams_count(db, limit=500):
    coll = db['threegrams-count-count']
    i = 0
    l = []
    for p in coll.aggregate([{'$group':{'_id':'$term', 'count':{'$sum':1 } } }, {'$sort':{'count':-1}}]):
        if i == limit:
            break
        l.append(str(p['_id']) + '\t' + str(p['count']) + '\n')
        i += 1
    with open('most_common_threegrams.txt', 'w') as f:
        f.writelines(l)


def connect_db(database='test'):
    '''
    return handle
    '''
    try:
        conn = pymongo.MongoClient()
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e 
    db = conn[database]
    return db


if __name__ == '__main__':
    db = connect_db()
    coll1 = db['web-index']
    coll2 = db['term-count']
    coll3 = db['threegrams-count']

    num_unique_pages(db)
    subdomain_count(db)
    longest_page(db)
    tokens_count(db, limit=500)
    threegrams_count(db, limit=20)


