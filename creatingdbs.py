import bs4
from bs4 import BeautifulSoup as bs
import glob
import json
import math
import networkx as nx
import operator
import pymongo
import re
import redis
import string
import sys
import time
import traceback
import urllib
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

termPositions = {}
num_Documents = 0

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

def extract_content_words(html=''):
    '''
    extract text content from html page
    '''
    page = bs(html, 'html.parser')
    texts = page.find_all(text=True)
    vis_text = [tokenize(str(element)) for element in texts \
                    if not element.parent.name in ['style', 'script', '[document]', 'head', 'title'] \
                    and not isinstance(element,bs4.element.Comment)]
    word_list = [stemmer.stem(word.strip()) for line in vis_text for word in line if word not in stopwords]
    return filter(None, word_list)


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

#################################################################################
def computePositions(docId,tokens): 
    docSize = len(tokens)
    tokenMap = positionMap(tokens)
    global num_Documents
    num_Documents += 1
    position_rows = [{'term':key, 'doc_id': docId, 'position': value, 'tf-idf': termFrequency(len(value),docSize)} for key,value in tokenMap.iteritems()]

    for entry in position_rows:
        postingEntry = {DB_TFIDF_DOCID: entry['doc_id'], DB_TFIDF_POSITION: entry['position'], DB_TFIDF_SCORE: entry['tf-idf']}
        if entry['term'] in termPositions:
            termPositions[entry['term']].append(postingEntry)
        else:
            termPositions[entry['term']] = [postingEntry]

def insertTFIDF():
    db = connect_db('IR')
    coll = db["tf-idf"]
    print "Number of documents : "
    print num_Documents
    termPostings = [{DB_TFIDF_TERM:key,DB_TFIDF_POSTING:value,DB_TFIDF_IDF:idf(len(value),num_Documents)} for key,value in termPositions.iteritems()]
    coll.insert_many(termPostings)

def termFrequency(freq,docSize):
    if(freq == 0): 
        return 0
    result = 1.0 + math.log1p(1.0 * freq)
    return result

def idf(termDocs,totalDocs):
    result = math.log1p((1.0 * totalDocs)/(termDocs+1))
    return result

def positionMap(tokens):
    tokenMap = {}
    for i in xrange(len(tokens)):
        if tokens[i] in tokenMap:
            tokenMap[tokens[i]].append(i)
        else:
            tokenMap[tokens[i]] = [i]
    return tokenMap

############################################################################################################

def extract_anchortext_title_info(doc_id, html='', url=''):
    try:
        db = connect_db('IR')
        rdb1 = redis.StrictRedis(db=URL_DOCID)
        page = bs(html, 'html.parser')
        anchors = page.find_all('a')
        anchortext_links = [[str(anchor.text).strip(), urlparse.urljoin(url, str(urllib.unquote(anchor['href']))).replace('https://', 'http://').strip('/')] \
                                                            for anchor in page.find_all('a') \
                                                                if anchor.has_attr('href') \
                                                                    and not anchor['href'].startswith('mailto') \
                                                                    and not anchor['href'].startswith('javascript')]
        
        # clean anhor text
        urltest = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+~]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        for i, ele in enumerate(anchortext_links):
            anchortext_links[i][0] = ' '.join(string_clean(word)
                                                for word in ele[0].split() \
                                                    if word not in re.findall(urltest, ele[0]) \
                                                        and len(word) > 1 )

        anchortext_info       = []
        anchortext_info_2gram = []
        for text, outlink in anchortext_links:
            if text != '' and is_valid_url(outlink):
                text_words = text.split(' ')
                for word in text_words:
                    if word not in stopwords:
                        anchortext_info.append({DB_ANCHOR_TEXT:word.strip(),
                                                DB_ANCHOR_TARGET: rdb1.get(outlink),
                                                DB_ANCHOR_SOURCE:doc_id})
                if len(text_words) > 1:
                    anchortext_info_2gram.extend(
                                             {DB_ANCHOR_TEXT :' '.join(text_words[i:i+2]),
                                              DB_ANCHOR_TARGET: rdb1.get(outlink),
                                              DB_ANCHOR_SOURCE:doc_id}
                                                 for i in xrange(len(text_words)-1) \
                                                    if text_words[i] not in stopwords and \
                                                        text_words[i+1] not in stopwords)

        # handle title
        title = page.title.string if page.title is not None and page.title.string is not None else ''
        title_words = title.split(' ')
        title_info = [{DB_TITLE_TERM: string_clean(word), DB_TITLE_DOCID: doc_id}
                                                            for word in title_words \
                                                                if word not in stopwords and len(word) > 1 ]
        title_info_2gram = []

        for i in xrange(len(title_words)-1):
            tmp = string_clean(title_words[i:i+2])
            tmp1 = tmp.split(' ')
            if len(tmp1) > 1 and tmp1[0] not in stopwords and tmp1[1] not in stopwords:
                title_info_2gram.append({DB_TITLE_TERM: tmp, DB_TITLE_DOCID: doc_id })

        coll_title = db['ti']
        if len(title_info) > 0:
            coll_title.insert_many(title_info)

        coll_title = db['ti_2g']
        if len(title_info_2gram) > 0:
            coll_title.insert_many(title_info_2gram)

        coll_anchor = db['an']
        if len(anchortext_info) > 0:
            coll_anchor.insert_many(anchortext_info)

        coll_anchor_2gram = db['an_2g']
        if len(anchortext_info_2gram) > 0:
            coll_anchor_2gram.insert_many(anchortext_info_2gram)
    except:
        print "\nURL: " + url
        print anchortext_links
        print
        print(sys.exc_info()[0])
        print traceback.print_exc()
        time.sleep(5)


def old_extract_anchortext_title_info(doc_id, html='', url=''):
    try:
        db = connect_db('IR')
        rdb1 = redis.StrictRedis(db=URL_DOCID)
        page = bs(html, 'html.parser')
        anchors = page.find_all('a')
        anchortext_links = [[str(anchor.text).strip(), urlparse.urljoin(url, str(urllib.unquote(anchor['href']))).replace('https://', 'http://').strip('/')] \
                                                            for anchor in page.find_all('a') \
                                                                if anchor.has_attr('href') \
                                                                    and not anchor['href'].startswith('mailto') \
                                                                    and not anchor['href'].startswith('javascript')]

        
        for i, ele in enumerate(anchortext_links):
            anchortext_links[i][0] = ' '.join(stemmer.stem(word) for word in ele[0].split(' ') if word not in stopwords)
        
        anchortext_info = [{DB_ANCHOR_TEXT:text.strip(), DB_ANCHOR_TARGET: rdb1.get(outlink), DB_ANCHOR_SOURCE:doc_id} \
                                for text, outlink in anchortext_links if text != '' and is_valid_url(outlink)]

        title = page.title.string if page.title is not None and page.title.string is not None else ''
        title = ' '.join([stemmer.stem(word) for word in title.split(' ') if word not in stopwords]).strip()
        title_info = {DB_TITLE_TERM:title, DB_TITLE_DOCID: doc_id}
    except:
        print anchortext_links
        print(sys.exc_info()[0])
        print
        return

    coll_title = db['title-docids']
    coll_title.insert_one(title_info)
    coll_anchor = db['anchortext-docids']
    if len(anchortext_info) > 0:
        coll_anchor.insert_many(anchortext_info)


def create_dbs():
    rdb1 = redis.StrictRedis(db=URL_DOCID)
    for f in glob.iglob('data/*'):
        with open(f, 'r') as fs:
            c = fs.read()
            url, html = c[:c.index('\n')].strip(), c[c.index('\n'):].strip()
            url = urllib.unquote(url)
            url = url.replace('https://', 'http://')
            doc_id = rdb1.get(url)
            terms = extract_content_words(html)
            computePositions(doc_id, terms)
            extract_anchortext_title_info(html=html, url=url, doc_id=doc_id)
            create_webgraph(doc_id=doc_id, html=html, url=url)
    insertTFIDF()

######################################################################################################


def extract_links(html='', url=''):
    '''
    get outgoing links from the page
    '''
    page  = bs(html, 'html.parser')
    anchors = page.find_all('a')
    links = [urlparse.urljoin(url, str(urllib.unquote(anchor['href']))).replace('https://', 'http://').strip('/') \
                                                        for anchor in page.find_all('a') \
                                                            if anchor.has_attr('href') \
                                                                and not anchor['href'].startswith('mailto') \
                                                                and not anchor['href'].startswith('javascript')]
    links = filter(is_valid_url, links)
    return set(links)


def is_valid_url(url):
    '''
    check if URL is a valid url
    '''
    url_struct = urlparse.urlparse(url)
    traps = []
    traps.append('/datasets/datasets' in url)
    traps.append('respond#respond' in url)
    traps.append(re.match('.*\?(.+=.+)&(.+=.+)&.*', url) is not None)
    traps.append(len(url) > 150)
    traps.append(len(url.split('/')) > 10)
    traps.append(url_struct.scheme not in ['http', 'https', ''])
    traps.append('action=login' in url)
    traps.append('ics.uci.edu' not in url)
    pattern = '.*\.(css|js|bmp|gif|jpe?g|ico|png|tiff?|mid|mp2|mp3|mp4' \
                + '|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf' \
                + '|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2' \
                + '|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1|xaml|pict' \
                + '|thmx|mso|arff|rtf|jar|csv|java|javac|cc|h|cpp|py|pyc|m|c|db|md5|nb|lif|xml|r|networks|odc' \
                + '|rm|smil|wmv|swf|wma|zip|rar|gz|lzip|lha|arj|wmz|pcz|lsp|pov|z|bib|rkt' \
                + '|3gp|amr|au|vox|rar|aac|ace|alz|apk|arc|txt|wcz|svm|ss|svg|Z|ma)$'
    traps.append(re.match(pattern, url_struct.path.lower()))
    traps.append(re.match(pattern, url.lower()))
    return not any(traps)


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



alllinks = set()
def create_docids_url_db():
    count = 0
    for f in glob.iglob('data_main/*'):
        with open(f, 'r') as fs:
            c = fs.read()
            url, html = c[:c.index('\n')].strip(), c[c.index('\n'):].strip()
            url = urllib.unquote(url)
            url = url.replace('https://', 'http://').strip('/')
            try:
                # validate url
                if not is_valid_url(url):
                    continue
                outlinks = extract_links(html=html, url=url)
                global alllinks
                alllinks.add(url)
                alllinks = alllinks.union(outlinks)
                count += 1
                if(count%1000 == 0):
                    print count
            except:
                print url
                print f
                print(sys.exc_info()[0])
                print
    rdb0 = redis.StrictRedis(db=DOCID_URL)
    rdb1 = redis.StrictRedis(db=URL_DOCID)
    docId = 0
    for link in alllinks:
        rdb0.set(docId,link)
        rdb1.set(link,docId)
        docId += 1
        if docId%1000 == 0:
            print docId 

rdb0 = redis.StrictRedis(db=DOCID_URL)
rdb1 = redis.StrictRedis(db=URL_DOCID)
def create_new_data():
    count = 0
    for f in glob.iglob('data_main/*'):
        with open(f, 'r') as fs:
            c = fs.read()
            url, html = c[:c.index('\n')].strip(), c[c.index('\n'):].strip()
            url = urllib.unquote(url)
            url = url.replace('https://', 'http://')
            try:
                # validate url
                if not is_valid_url(url):
                    continue
                docid = rdb1.get(url)
                with open('data/f'+str(docid), 'w') as fw:
                    fw.write(c)
            except:
                print url
                print f
                print(sys.exc_info()[0])
                print



def create_webgraph(doc_id, html='', url=''):
    try:
        db = connect_db('IR')
        rdb1 = redis.StrictRedis(db=URL_DOCID)
        page = bs(html, 'html.parser')
        anchors = page.find_all('a')
        anchortext_links = [[str(anchor.text).strip(), urlparse.urljoin(url, str(urllib.unquote(anchor['href']))).replace('https://', 'http://').strip('/')] \
                                                            for anchor in page.find_all('a') \
                                                                if anchor.has_attr('href') \
                                                                    and not anchor['href'].startswith('mailto') \
                                                                    and not anchor['href'].startswith('javascript')]

        
        anchortext_info = [{DB_ANCHOR_TEXT:text.strip(), DB_ANCHOR_TARGET: rdb1.get(outlink), DB_ANCHOR_SOURCE:doc_id} \
                                for text, outlink in anchortext_links if text != '' and is_valid_url(outlink)]

    except:
        print anchortext_links
        print(sys.exc_info()[0])
        print
        return

    coll_anchor = db['webgraph']
    if len(anchortext_info) > 0:
        coll_anchor.insert_many(anchortext_info)


def constructGraph():
    db = connect_db('IR')
    coll_anchor = db['webgraph']
    G = nx.DiGraph()
    cursor = coll_anchor.find()
    edgeList = [(doc[DB_ANCHOR_SOURCE],doc[DB_ANCHOR_TARGET]) for doc in cursor if doc[DB_ANCHOR_SOURCE]]
    #print edgeList
    G.add_edges_from(edgeList)
    return G

def computePageRank():
    G = constructGraph()
    results = nx.pagerank(G)
    #sorted_result = sorted(result.items(), key = operator.itemgetter(1), reverse = True)
    rdb3 = redis.StrictRedis(db=DOCID_PAGERANK)
    for doc_id, pagerank in results.iteritems():
        rdb3.set(doc_id, pagerank)
    return results


print "Creating URL,DOCID map"
#create_docids_url_db()
print "URL,DOCID map created"
#create_new_data()
print "EXTRACTED FILTERED DATA FILES"
#create_dbs()
computePageRank()
print "Done Indexing!"
print





