#!/usr/bin/python-2.7

###
# CS221 : Information Retrieval - Winter 2016
# Project 1
# @author: karthikprasad
# @date: 18 Jan 2016
#
# Functions for the parts requored in Project 1 of the course
#
###

import collections as coll
import redis
import nltk
import string

# the following variable refers to a list of words obtained from http://www.keithv.com/software/wlist/
BIG_WL = '../data/big_wl'

############################# SOME PRINT UTILS ################################

def print_list(some_list):
    print some_list

def print_iter(some_iterable):
    for ele in some_iterable:
        print ele,
    print

def print_iter_sep_line(some_iterable):
    for ele in some_iterable:
        print ele


############################# TOKENIZE A GIVEN FILE ###########################

def tokenize_file(filename):
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
    with open(filename, 'r') as f:
        for line in f:
            tokens.extend(line.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split())
    return tokens

def print_tokens(tokens):
    '''
    @input: list of strings
    '''
    print_list(tokens)


####################### COMPUTE WORDS AND ITS FREQUENCIES #####################

def compute_word_frequencies(word_list):
    '''
    @input: list of words
    @return: list of word-freq pairs in descending order of freq
    '''
    return coll.Counter(word_list).most_common()

def print_word_frequency(word_freq_pairs):
    '''
    @input: list of pairs
    '''
    print_iter_sep_line(word_freq_pairs)


###################### COMPUTE 3-GRAMS AND ITS FREQUENCIES ####################

def compute_ngrams(tokens, n=3):
    '''
    compute all the ngrams and return a generator for faster processing
    @input: list of tokens
    @return: generator object yielding ngrams
    '''
    return (tuple(tokens[i:i+n]) for i in xrange(len(tokens)-n))

def compute_3grams_frequencies(tokens):
    '''
    compute 3 grams and return all the ngram-freq in descending sorted order
    @input: list of tokens
    @return: list of 3gram-freq pairs in descending order of freq
    '''
    return coll.Counter(compute_ngrams(tokens, 3)).most_common()

def print_ngrams(ngram_freq_pairs):
    '''
    @input: list of pairs
    '''
    print_list(ngram_freq_pairs)


#################### BUILD ANAGRAMS AND PERSIST IN REDIS ######################

def build_anagramsdb():
    '''
    build a db of sorted-word and the set of words and load it into redis
    Using brown corpus and a file containing words from multiple corpora
    strip all punctuations, retain hyphens, replace underscores with space

    '''
    rdb = redis.StrictRedis()
    rdb.flushdb()
    punctuations_replace = '#"(){}[]<>.+,/:;=?@_|~-'
    punctuations_remove = '!$\'%&\\*^`'
    # 1. Brown Corpus
    for word in nltk.corpus.brown.words():
        w = str(word)
        # polish just the way tokens were
        w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
        for each_w in w_list:
            # add the word to redis with key as a sorted word
            rdb.sadd(''.join(sorted(each_w)), each_w)
    # 2. Wordnet
    for word in nltk.wordnet.wordnet.words():
        w = str(word)
        # polish just the way tokens were
        w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
        for each_w in w_list:
            # add the word to redis with key as a sorted word
            rdb.sadd(''.join(sorted(each_w)), each_w)
    # 3. Other corpora
    with open(BIG_WL, 'r') as f:
        for line in f:
            w = str(line).strip()
            # polish just the way tokens were
            w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
            for each_w in w_list:
                # add the word to redis with key as a sorted word
                rdb.sadd(''.join(sorted(each_w)), each_w)

def detect_anagrams_in_db(tokens):
    '''
    for each token, create a pair of token and set of anagrams of this word
    @input: list of tokens
    @return: generator yielding token-anagramset pairs
    '''
    rdb = redis.StrictRedis()
    return ((token, rdb.smembers(''.join(sorted(token)))) for token in set(tokens))


###################### BUILD ANAGRAMS IN MEMORY USING A HASH MAP ########################

wam = coll.defaultdict(set)
def build_anagrams():
    '''
    build a python dict of sorted-word and the set of words
    Using brown corpus and a file containing words from multiple corpora
    strip all punctuations, retain hyphens, replace underscores with space
    '''
    punctuations_replace = '#"(){}[]<>.+,/:;=?@_|~-'
    punctuations_remove = '!$\'%&\\*^`'
    # 1. Brown Corpus
    for word in nltk.corpus.brown.words():
        w = str(word)
        # polish just the way tokens were
        w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
        for each_w in w_list:
            # add the word to redis with key as a sorted word
            wam[''.join(sorted(each_w))].add(each_w)
    # 2. Wordnet
    for word in nltk.wordnet.wordnet.words():
        w = str(word)
        # polish just the way tokens were
        w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
        for each_w in w_list:
            # add the word to redis with key as a sorted word
            wam[''.join(sorted(each_w))].add(each_w)
    # 3. Other corpora
    with open(BIG_WL, 'r') as f:
        for line in f:
            w = str(line).strip()
            # polish just the way tokens were
            w_list = w.translate(string.maketrans(punctuations_replace,' '*len(punctuations_replace)), punctuations_remove).strip().lower().split()
            for each_w in w_list:
                # add the word to redis with key as a sorted word
                wam[''.join(sorted(each_w))].add(each_w)

def detect_anagrams(tokens):
    '''
    for each token, create a pair of token and set of anagrams of this word
    @input: list of tokens
    @return: generator yielding token-anagramset pairs
    '''
    return ((token, wam[''.join(sorted(token))]) for token in set(tokens))

def print_anagrams(token_anagram_pairs):
    '''
    @input: generator of string-set pairs
    '''
    print_iter_sep_line(token_anagram_pairs)

###############################################################################
