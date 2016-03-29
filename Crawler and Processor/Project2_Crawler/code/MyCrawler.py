'''
@author: {prasadkr,pmantrip,rishabas}@uci.edu
'''

from Crawler4py.Crawler import Crawler
from MyCrawlerConfig import MyCrawlerConfig
import time
import os

directory = "html_files"
if not os.path.exists(directory):
    os.makedirs(directory)

crawler = Crawler(MyCrawlerConfig())
print ("Start timestamp: " + time.strftime("%c"))
print (crawler.StartCrawling())
print ("Stop timestamp: " + time.strftime("%c"))

exit(0)