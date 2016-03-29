import bs4
from bs4 import BeautifulSoup as bs
import glob
import json
import pymongo
import re
import string
import sys
import urlparse
reload(sys)
sys.setdefaultencoding('utf-8')


stopwords = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'arent', 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'cant', 'cannot', 'could', 'couldnt', 'did', 'didnt', 'do', 'does', 'doesnt', 'doing', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', 'hadnt', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hed', 'hell', 'hes', 'her', 'here', 'heres', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'hows', 'i', 'id', 'ill', 'im', 'ive', 'if', 'in', 'into', 'is', 'isnt', 'it', 'its', 'its', 'itself', 'lets', 'me', 'more', 'most', 'mustnt', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours  ourselves', 'out', 'over', 'own', 'same', 'shant', 'she', 'shed', 'shell', 'shes', 'should', 'shouldnt', 'so', 'some', 'such', 'than', 'that', 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', 'theres', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasnt', 'we', 'wed', 'well', 'were', 'weve', 'were', 'werent', 'what', 'whats', 'when', 'whens', 'where', 'wheres', 'which', 'while', 'who', 'whos', 'whom', 'why', 'whys', 'with', 'wont', 'would', 'wouldnt', 'you', 'youd', 'youll', 'youre', 'youve', 'your', 'yours', 'yourself', 'yourselves']

def tokenize(text):
    '''
    tokenize the text in the given file. Ignore cases.
    remove commas, semi-colons, hyphens and other punctuations.
    split on spaces

    @input: name of the text file
    @return: list of tokens in the file.
    '''
    tokens = []
    punctuations_replace = '#"(){}[]<>.+,/:;=?@_|~-'
    punctuations_remove = '!$\'%&\\*^`'
    tokens.extend(text.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split())
    return tokens


def compute_ngrams(tokens):
    '''
    compute all the ngrams and return a generator for faster processing
    @input: list of tokens
    @return: list object yielding ngrams
    '''
    return [{'three_gram':' '.join(tokens[i:i+3]), 'count':1} for i in xrange(len(tokens)-3) \
                if tokens[i] not in stopwords and \
                   tokens[i+1] not in stopwords and \
                   tokens[i+2] not in stopwords]


def extract_content_words(html='',page=None):
    '''
    extract text content from html page
    '''
    if page is None:
        page = bs(html, 'html.parser')
    texts = page.find_all(text=True)
    vis_text = [tokenize(str(element)) for element in texts \
                    if not element.parent.name in ['style', 'script', '[document]', 'head', 'title'] \
                    and not isinstance(element,bs4.element.Comment)]
    word_list = [word.strip() for line in vis_text for word in line]
    return filter(None, word_list)


def extract_title(html='', page=None):
    '''
    get title of page
    '''
    if page is None:
        page = bs(html, 'html.parser')
    return page.title.string if page.title is not None else None



def extract_info(html, url):
    '''
    get all info about a page
    '''
    page = bs(html, 'html.parser')
    # TITLE
    title = extract_title(page=page)
    # TEXT CONTENT WORDLIST
    word_list = extract_content_words(page=page)
    # OUTGOING LINKS
    #links = extract_links(page=page, url=url)
    #return (title, word_list, links)
    return (title, word_list, [])



def extract_links(html='', page=None, url=''):
    '''
    get outgoing links from the page
    '''
    if page is None:
        page  = bs(html, 'html.parser')
    anchors = page.find_all('a')
    links = [urlparse.urljoin(url, str(anchor['href']) ) for anchor in page.find_all('a') \
                                                            if anchor.has_attr('href') \
                                                                and not anchor['href'].startswith('mailto') \
                                                                and not anchor['href'].startswith('javascript')]
    return links



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


def extract_domain(url):
    '''
    get domain from url
    '''
    url_struct = urlparse.urlparse(url)
    return url_struct.hostname


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


db = connect_db(database='IR')
coll1 = db['web-index']
coll2 = db['term-count']
coll3 = db['threegrams-count']

seen_urls = json.load(open('seen_urls.json', 'r'))
doc_id = 1
def process_raw_files(folder='data/'):
    cnt = 0
    for f in glob.iglob(folder+'*'):
        cnt += 1
        if (cnt % 500 == 0):
            print cnt
            print f
        with open(f, 'r') as fs:
            c = fs.read()
            url, html = c[:c.index('\n')].strip(), c[c.index('\n'):].strip()
            try:
                # validate url
                if not is_valid_url(url):
                    continue
                if url in seen_urls:
                    continue
                title, wordlist, outlinks = extract_info(html, url)
                if not title or len(wordlist) <= 0:
                    #print url
                    continue
                global doc_id
                doc = {}
                doc['doc_id'] = doc_id
                doc['url'] = url
                doc['subdomain'] = extract_domain(url)
                doc['wordlist']  = wordlist
                doc['num_words'] = len(wordlist)
                #doc['outlinks']  = outlinks
                # term Freq
                good_words = [{'term':word, 'count':1} for word in wordlist if word not in stopwords]
                if len(good_words) > 0:
                    coll2.insert_many(good_words)
                # 3 Grams
                ngrams = compute_ngrams(wordlist)
                if len(ngrams) > 0:
                    coll3.insert_many(ngrams)
                
                doc_id += 1
                seen_urls[url] = doc_id
            except:
                print url
                print f
                print(sys.exc_info()[0])
                print


for d in glob.glob('data/*'):
    process_raw_files(d+'/')
json.dump(seen_urls, open('seen_urls.json', 'w'))

