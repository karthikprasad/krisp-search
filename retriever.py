import bs4
from bs4 import BeautifulSoup as bs
import glob
import heapq
import itertools
import json
import pymongo
import math
#import networkx as nx
from pprint import pprint
import operator
import os
import re
import redis
import string
import sys
import urlparse


from nltk.stem import SnowballStemmer
stemmer = SnowballStemmer("english")
reload(sys)
sys.setdefaultencoding('utf-8')

DOCID_URL = 1
URL_DOCID = 2
DOCID_PAGERANK = 3

# tf-idf key names
DB_TFIDF_TERM = 't'
DB_TFIDF_POSTING = 'l'
DB_TFIDF_DOCID = 'd'
DB_TFIDF_POSITION = 'p'
DB_TFIDF_SCORE = 's'
DB_TFIDF_IDF = 'i'
# title doc key names
DB_TITLE_TERM = 't'
DB_TITLE_DOCID = 'd'
# anchor text key names
DB_ANCHOR_TEXT = 'a'
DB_ANCHOR_SOURCE = 's'
DB_ANCHOR_TARGET = 't'

stopwords = ['a', 'able', 'about', 'above', 'abst', 'accordance', 'according', 'accordingly', 'across', 'act', 'actually', 'added', 'adj', 'affected', 'affecting', 'affects', 'after', 'afterwards', 'again', 'against', 'ah', 'all', 'almost', 'alone', 'along', 'already', 'also', 'although', 'always', 'am', 'among', 'amongst', 'an', 'and', 'announce', 'another', 'any', 'anybody', 'anyhow', 'anymore', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apparently', 'approximately', 'are', 'aren', 'arent', 'arise', 'around', 'as', 'aside', 'ask', 'asking', 'at', 'auth', 'available', 'away', 'awfully', 'b', 'back', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 'beforehand', 'begin', 'beginning', 'beginnings', 'begins', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 'between', 'beyond', 'biol', 'both', 'brief', 'briefly', 'but', 'by', 'c', 'ca', 'came', 'can', 'cannot', 'cant', 'cause', 'causes', 'certain', 'certainly', 'co', 'com', 'come', 'comes', 'contain', 'containing', 'contains', 'could', 'couldnt', 'd', 'date', 'did', 'didnt', 'different', 'do', 'does', 'doesnt', 'doing', 'done', 'dont', 'down', 'downwards', 'due', 'during', 'e', 'each', 'ed', 'edu', 'effect', 'eg', 'eight', 'eighty', 'either', 'else', 'elsewhere', 'end', 'ending', 'enough', 'especially', 'et', 'et-al', 'etc', 'even', 'ever', 'every', 'everybody', 'everyone', 'everything', 'everywhere', 'ex', 'except', 'f', 'far', 'few', 'ff', 'fifth', 'first', 'five', 'fix', 'followed', 'following', 'follows', 'for', 'former', 'formerly', 'forth', 'found', 'four', 'from', 'further', 'furthermore', 'g', 'gave', 'get', 'gets', 'getting', 'give', 'given', 'gives', 'giving', 'go', 'goes', 'gone', 'got', 'gotten', 'h', 'had', 'happens', 'hardly', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hed', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'heres', 'hereupon', 'hers', 'herself', 'hes', 'hi', 'hid', 'him', 'himself', 'his', 'hither', 'home', 'how', 'howbeit', 'however', 'hundred', 'i', 'id', 'ie', 'if', 'ill', 'im', 'immediate', 'immediately', 'importance', 'important', 'in', 'inc', 'indeed', 'index', 'information', 'instead', 'into', 'invention', 'inward', 'is', 'isnt', 'it', 'itd', 'itll', 'its', 'itself', 'ive', 'j', 'just', 'k', 'keep  keeps', 'kept', 'kg', 'km', 'know', 'known', 'knows', 'l', 'largely', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', 'lets', 'like', 'liked', 'likely', 'line', 'little', 'll', 'look', 'looking', 'looks', 'ltd', 'm', 'made', 'mainly', 'make', 'makes', 'many', 'may', 'maybe', 'me', 'mean', 'means', 'meantime', 'meanwhile', 'merely', 'mg', 'might', 'million', 'miss', 'ml', 'more', 'moreover', 'most', 'mostly', 'mr', 'mrs', 'much', 'mug', 'must', 'my', 'myself', 'n', 'na', 'name', 'namely', 'nay', 'nd', 'near', 'nearly', 'necessarily', 'necessary', 'need', 'needs', 'neither', 'never', 'nevertheless', 'new', 'next', 'nine', 'ninety', 'no', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'nor', 'normally', 'nos', 'not', 'noted', 'nothing', 'now', 'nowhere', 'o', 'obtain', 'obtained', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'omitted', 'on', 'once', 'one', 'ones', 'only', 'onto', 'or', 'ord', 'other', 'others', 'otherwise', 'ought', 'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'owing', 'own', 'p', 'page', 'pages', 'part', 'particular', 'particularly', 'past', 'per', 'perhaps', 'placed', 'please', 'plus', 'poorly', 'possible', 'possibly', 'potentially', 'pp', 'predominantly', 'present', 'previously', 'primarily', 'probably', 'promptly', 'proud', 'provides', 'put', 'q', 'que', 'quickly', 'quite', 'qv', 'r', 'ran', 'rather', 'rd', 're', 'readily', 'really', 'recent', 'recently', 'ref', 'refs', 'regarding', 'regardless', 'regards', 'related', 'relatively', 'research', 'respectively', 'resulted', 'resulting', 'results', 'right', 'run', 's', 'said', 'same', 'saw', 'say', 'saying', 'says', 'sec', 'section', 'see', 'seeing', 'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sent', 'seven', 'several', 'shall', 'she', 'shed', 'shell', 'shes', 'should', 'shouldnt', 'show', 'showed', 'shown', 'showns', 'shows', 'significant', 'significantly', 'similar', 'similarly', 'since', 'six', 'slightly', 'so', 'some', 'somebody', 'somehow', 'someone', 'somethan', 'something', 'sometime', 'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specifically', 'specified', 'specify', 'specifying', 'still', 'stop', 'strongly', 'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure   t', 'take', 'taken', 'taking', 'tell', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', 'thatll', 'thats', 'thatve', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'thered', 'therefore', 'therein', 'therell', 'thereof', 'therere', 'theres', 'thereto', 'thereupon', 'thereve', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'think', 'this', 'those', 'thou', 'though', 'thoughh', 'thousand', 'throug', 'through', 'throughout', 'thru', 'thus', 'til', 'tip', 'to', 'together', 'too', 'took', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'ts', 'twice', 'two', 'u', 'un', 'under', 'unfortunately', 'unless', 'unlike', 'unlikely', 'until', 'unto', 'up', 'upon', 'ups', 'us', 'use', 'used', 'useful', 'usefully', 'usefulness', 'uses', 'using', 'usually', 'v', 'value', 'various', 've', 'very', 'via', 'viz', 'vol', 'vols', 'vs', 'w', 'want', 'wants', 'was', 'wasnt', 'way', 'we', 'wed', 'welcome', 'well', 'went', 'were', 'werent', 'weve', 'what', 'whatever', 'whatll', 'whats', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'wheres', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whim', 'whither', 'who', 'whod', 'whoever', 'whole', 'wholl', 'whom', 'whomever', 'whos', 'whose', 'why', 'widely', 'willing', 'wish', 'with', 'within', 'without', 'wont', 'words', 'world', 'would', 'wouldnt', 'www', 'x', 'y', 'yes', 'yet', 'you', 'youd', 'youll', 'your', 'youre', 'yours', 'yourself', 'yourselves', 'youve', 'z', 'zero']

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

def string_clean(words):
    result = []
    replace_punc_space = ':;=?@_|~#"<>.+,/(){}[]-\n'
    remove_punctuations = '&\\*^`!$\'%'
    if type(words) is list:
        words = ' '.join(words)

    for word in words.split(' '):
        tmp = str(stemmer.stem(word)).translate(None, remove_punctuations).translate( \
                  string.maketrans(replace_punc_space,' '*len(replace_punc_space))).strip()
        result.append(tmp if tmp else None)
    result = filter(None, result)
    return ' '.join(result).strip()

def tokenize(text):
    '''
    tokenize the text in the given file. Ignore cases.
    remove commas, semi-colons, hyphens and other punctuations.
    split on spaces

    @input: name of the text file
    @return: list of tokens in the file.
    '''
    tokens = []
    punctuations_replace = '#"(){}[]<>.+,/:;=?@_|~-\n'
    punctuations_remove = '!$\'%&\\*^`'
    tokens.extend(text.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split())
    return tokens

def preProcess(query):
    vis_text = tokenize(str(query))
    word_list = [stemmer.stem(word.strip()) for word in vis_text if word not in stopwords]
    return filter(None, word_list)

def getTfIdfScores(queryWords):
    print queryWords
    db = connect_db('IR')
    coll = db['tf-idf']
    docList = {}
    for word in queryWords:
        cursor = coll.find_one({DB_TFIDF_TERM:word})
        if not cursor:
            continue
        for entry in cursor[DB_TFIDF_POSTING]:
            score = entry[DB_TFIDF_SCORE] * cursor[DB_TFIDF_IDF]
            docID = entry[DB_TFIDF_DOCID]
            if docID is None:
                print
                print word
                print '########'
            if docID in docList:
                docList[docID] += score
            else:
                docList[docID] = score
    m1, m2 = min(docList.values()), max(docList.values())
    if m2-m1 > 0:
        for d in docList:
            docList[d] /= (m2 - m1)
    '''
    for doc_id, score in docList.iteritems():
        docList[doc_id] = 1.0/(1+math.exp(-score))
    '''
    return docList

def getTopNResults(docList,n = 10):
    sorted_result = sorted(docList.items(), key=operator.itemgetter(1), reverse=True)
    result = [x[0] for x in sorted_result]
    return result[:n]

def parse_docIDS(list_docids, query, popular_word=""):
    query = re.sub("[^\w]", " ",  query).lower().split()
    print
    print query
    result = []
    if not list_docids:
        return None
    rdb0 = redis.StrictRedis(db=DOCID_URL)
    print [rdb0.get(docID) for docID in list_docids]
    for docID in list_docids:
        missing_words = []
        f = "data/f" + docID
        if not os.path.isfile(f):
            url = rdb0.get(docID)
            result.append({'link': url, \
                        'title': url, \
                        'desc': 'Blank Summary', \
                        'missing': None,\
                        'bold': ''
                      })
            continue
        with open(f, 'r') as fs:
            html = fs.read()
            url = rdb0.get(docID)
            page = bs(html, 'html.parser')
            title = page.title.string if page.title is not None and page.title.string is not None else url

            texts = page.find_all(text=True)
            word_list = [tokenize(str(element)) for element in texts \
                        if not element.parent.name in ['style', 'script', '[document]', 'head', 'title'] \
                        and not isinstance(element,bs4.element.Comment)]
            word_list = filter(None, word_list)
            word_list = list(itertools.chain(*word_list))
            word_list.extend(title.split(' '))
            word_list_2 = [stemmer.stem(word) for word in word_list]
            missing_words = [word for word in query if word not in word_list and word not in word_list_2]
            tmp_index = 0
            for word in query:
                if word in word_list:
                    tmp_index = word_list.index(word)
            lower_bound = 0
            if tmp_index > 9:
                lower_bound = tmp_index-8
            summary = ' '.join(word_list[lower_bound: tmp_index+8])
            bold_word = [word for word in query if word in summary]
            for word in bold_word:
                summary = summary.replace(word, '<b>'+word+'</b>')
            try:
                summary = unicode(summary, errors='replace')
            except:
                summary = summary
            result.append({'link': url, \
                           'title': title, \
                           'desc': summary, \
                           'missing': ', '.join(missing_words) if len(missing_words) else None,\
                           'bold': bold_word
                         })
        #print url
    return result

def at_score_helper(result, db="", db_name="", text_col="", target_docid_col="", query="", importance=""):
    docs = db[db_name].find({text_col: query})
    target_docs = [d[target_docid_col] for d in docs]
    if len(target_docs) == 0:
        return
    score = importance / len(target_docs)
    for d in target_docs:
        if result.has_key(d):
            result[d] = min(3, result[d] + score)
            #result[d] = max(0.25, result[d] + score)
        else:
            result[d] = score


def getAnchorTitleScores(query):
    result_a = {}
    result_t = {}
    query = string_clean(query).split()
    db = connect_db('IR')
    for i in range(len(query)):
        if i+1 != len(query) and query[i] not in stopwords and query[i+1] not in stopwords:
            q = query[i] + " " + query[i+1]
            at_score_helper(db=db, db_name="an_2g", text_col=DB_ANCHOR_TEXT, target_docid_col=DB_ANCHOR_TARGET, query=q, result=result_a, importance=2.0)
            at_score_helper(db=db, db_name="ti_2g", text_col=DB_TITLE_TERM, target_docid_col=DB_TITLE_DOCID, query=q, result=result_t, importance=2.0)

        if query[i] not in stopwords:
            at_score_helper(db=db, db_name="an", text_col=DB_ANCHOR_TEXT, target_docid_col=DB_ANCHOR_TARGET, query=query[i], result=result_a, importance=1.0)
            at_score_helper(db=db, db_name="ti", text_col=DB_TITLE_TERM, target_docid_col=DB_TITLE_DOCID, query=query[i], result=result_t, importance=1.0)            


    keys = heapq.nlargest(3, result_a, key=result_a.get)
    for k in keys:
        if len(result_a.keys()) < 7:
            result_a[k] = max(1.25, result_a[k])
        elif len(result_a.keys() > 7 and result_a.keys()) < 30:
            result_a[k] = max(0.20, result_a[k])
        else:
            result_a[k] = max(0.05, result_a[k])

    return result_a,result_t


def positionScore(distance):
    alpha = 0.3
    return math.log1p(alpha+math.exp(-1.0*distance))


def computeMinDistance(termPositions):
    if(len(termPositions) < 2):
        return 0

    merged_positions = []
    # Merge position lists
    for term,positionList in termPositions.items():
        for position in positionList: 
            merged_positions.append((term,position))

    def getKey(item):
        return item[1]
    merged_positions = sorted(merged_positions,key=getKey)

    # Compute min distance
    possible_values = [(merged_positions[i+1][1]-merged_positions[i][1] - 1) for i in xrange(len(merged_positions) - 1) if merged_positions[i][0] != merged_positions[i+1][0]]
    return min(possible_values)


def getPositionScores(queryWords):
    if(len(queryWords) < 2):
        return {}
    db = connect_db('IR')
    coll = db['tf-idf']
    docList = {} #{docID:[{term:position}]}
    for word in queryWords:
        cursor = coll.find_one({DB_TFIDF_TERM:word})
        if not cursor:
            continue
        for entry in cursor[DB_TFIDF_POSTING]:
            docID = entry[DB_TFIDF_DOCID]
            if docID in docList:
                docList[docID][word] = entry[DB_TFIDF_POSITION]
            else:
                docList[docID] = {word:entry[DB_TFIDF_POSITION]}

    # Compute position score for each document
    doc_scores = {}
    for docID,termPositions in docList.items():
        if(len(termPositions) >= 2):
            minDistance = computeMinDistance(termPositions)
            if docID not in doc_scores:
                doc_scores[docID] = positionScore(minDistance)
    return doc_scores


def getPageRankScores(doc_ids):
    rdb3 = redis.StrictRedis(db=DOCID_PAGERANK)
    pagerank_scores = {}
    for doc_id in doc_ids:
        score = rdb3.get(doc_id)
        pagerank_scores[doc_id] = float(score) if score is not None else 0
    return pagerank_scores


def processQuery(query):
    results = {}
    words = preProcess(query)
    # ANCHOR text, TITLE text
    results['anchor'], results['title'] = getAnchorTitleScores(query)
    # POSITION Score
    results['pos'] = getPositionScores(words)
    # TF-IDF
    results['tfidf'] = getTfIdfScores(words)
    # PAGERANKS
    doc_ids = set(results['anchor'].keys() + results['tfidf'].keys())
    results['pagerank'] = getPageRankScores(doc_ids)
    # CREATE BUCKETS
    bucket1, bucket2, bucket3 = bucketize(results, {})
    
    bucket = bucket1.copy()
    bucket.update(bucket2)
    bucket.update(bucket3)
    #print bucket
    #docList = getTfIdfScores(words)
    search_results = getTopNResults(bucket)
    return parse_docIDS(search_results, query)


def compute_score(doc_id, results, weights, eval_style = 1):
    # for linear combination
    if eval_style == 1:
        score = 0
        if results['anchor'].has_key(doc_id):
            score += weights['anchor']*results['anchor'][doc_id]
        if results['title'].has_key(doc_id):
            score += weights['title']*results['title'][doc_id]
        if results['pos'].has_key(doc_id):
            score += weights['pos']*results['pos'][doc_id]
        if results['tfidf'].has_key(doc_id):
            score += weights['tfidf']*results['tfidf'][doc_id]
        if results['pagerank'].has_key(doc_id):
            score += weights['pagerank']*results['pagerank'][doc_id]
        return score
        # return weights['anchor']*results['anchor'][doc_id] if results['anchor'].has_key(doc_id) else 0 + \
        #         weights['title']*results['title'][doc_id] if results['anchor'].has_key(doc_id) else 0 + \
        #         weights['pos']*results['pos'][doc_id] if results['anchor'].has_key(doc_id) else 0 + \
        #         weights['tfidf']*results['tfidf'][doc_id] if results['anchor'].has_key(doc_id) else 0 + \
        #         weights['pagerank']*results['pagerank'][doc_id] if results['anchor'].has_key(doc_id) else 0
                
    else: # HM,expo, etc.
        return 0

def bucketize(results, weights={}, N = 100):
    weights = {'anchor': 2.5, 'title': 2, 'pos': 2.0, 'tfidf': 1.0, 'pagerank':10}
    pprint(results)
    bucket1, bucket2, bucket3= {}, {}, {}
    # popluate bucket 1. doc id is present in both anchor and title
    bucket1_docids = set(results['anchor'].keys()).intersection(results['title'].keys())
    
    for doc_id in bucket1_docids:
        bucket1[doc_id] = compute_score(doc_id, results, weights)
    if len(bucket1) > N:
        return bucket1, bucket2, bucket3
    
    # Now popluate bucket 2. doc id is present in only one of anchor or title
    anchor_docids = set(results['anchor'].keys())
    title_docids = set(results['title'].keys())
    bucket2_docids = anchor_docids.union(title_docids).difference(bucket1_docids)
    for doc_id in bucket2_docids:
        bucket2[doc_id] = compute_score(doc_id, results, weights)
    if len(bucket1) + len(bucket2) > N:
        return bucket1, bucket2, bucket3

    # Now populate bucket 3 based on pos scores, tfidf and pagerank
    bucket3_docids = set(results['tfidf'].keys())
    bucket3_docids = bucket3_docids.difference(anchor_docids, title_docids)
    for doc_id in bucket3_docids:
        bucket3[doc_id] = compute_score(doc_id, results, weights)

    return bucket1, bucket2, bucket3



if __name__ == '__main__':
    rdb1 = redis.StrictRedis(db=DOCID_URL)
    # position scores
    # query = 'Videira Loepes'
    # resp = getPositionScores(preProcess(query))
    # resp = sorted(resp.items(), key = operator.itemgetter(1), reverse = True)
    # for d in resp:
    #     print rdb1.get(d[0]), d[1]

    resa,rest = getAnchorTitleScores('mondego')
    resa = sorted(resa.items(), key = operator.itemgetter(1), reverse = True)
    rest = sorted(rest.items(), key = operator.itemgetter(1), reverse = True)
    print 'anchor scores'
    for d in resa:
        print rdb1.get(d[0]), d[1]
    print
    print 'title scores'
    for d in rest:
        print rdb1.get(d[0]), d[1]

