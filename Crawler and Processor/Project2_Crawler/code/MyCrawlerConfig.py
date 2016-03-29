'''
@Author: {prasadkr,pmantrip,rishabas}@uci.edu
'''
from Crawler4py.Config import Config
import re
import sys
try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs

reload(sys)
sys.setdefaultencoding('utf-8')

count = 0
dir = "html_files/"

class MyCrawlerConfig(Config):
    def __init__(self):
        Config.__init__(self)
        self.UserAgentString = "IR W16 WebCrawler 85686586 42686317 79403075"

    def GetSeeds(self):
        '''Returns the first set of urls to start crawling from'''
        return ["http://www.ics.uci.edu"]


    def HandleData(self, parsedData):
        '''Function to handle url data. Guaranteed to be Thread safe.
        parsedData = {"url" : "url", "text" : "text data from html", "html" : "raw html data"}
        Advisable to make this function light. Data can be massaged later. Storing data probably is more important'''
        global count
        count += 1
        count_s = str(count)

        print(count_s + " ->  " + parsedData["url"])

        file1 = open('all_urls.txt', 'ab+')
        file1.write(count_s + " ->  " + parsedData["url"] + "\n")
        file1.close()

        file2 = open(dir + count_s + '.html', 'wb+')
        file2.write(parsedData["url"] + "\n")
        file2.write(parsedData["html"])
        file2.close()

        return

    def ValidUrl(self, url):
        '''Function to determine if the url is a valid url that should be fetched or not.'''
        parsed = urlparse(url)
        try:
            traps = []
            traps.append('/datasets/datasets' in url)
            traps.append("archive.ics.uci.edu" in parsed.hostname and "/ml/" in parsed.path)
            traps.append("drzaius.ics.uci.edu" in parsed.hostname and "/cgi-bin/cvsweb.cgi/" in parsed.path)
            traps.append("ironwood.ics.uci.edu" in parsed.hostname)
            traps.append("djp3-pc2.ics.uci.edu" in parsed.hostname and "/LUCICodeRepository/" in parsed.path)
            traps.append("www.ics.uci.edu" in parsed.hostname and "/~xhx/project/MotifMap/" in parsed.path)
            traps.append("duttgroup.ics.uci.edu" in parsed.hostname)
            traps.append("respond#respond" in url)
            traps.append("resultsgdgridv4" in url)
            traps.append("wics.ics.uci.edu" in url)
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
            traps.append(re.match(pattern, parsed.path.lower()))
            traps.append(re.match(pattern, url.lower()))
            return not any(traps)
        except TypeError:
            print ("TypeError for ", parsed)
