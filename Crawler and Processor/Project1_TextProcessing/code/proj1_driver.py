#!/usr/bin/python-2.7

###
# CS221 : Information Retrieval - Winter 2016
# Project 1
# @author: karthikprasad
# @date: 18 Jan 2016
#
# Driver to run project 1 of the course. Functions are defined in utils.py
#
###
from optparse import OptionParser as OP
from utils import *
import sys
import time

def partA(filename):
	t1 = time.time()
	tokens = tokenize_file(filename)
	t2 = time.time()
	print_tokens(tokens)
	t3 = time.time()
	print
	print 'Time taken to tokenize_file : ' + str(t2-t1) + ' seconds'
	print 'Time taken to print tokens  : ' + str(t3-t2) + ' seconds'


def partB(filename):
	t1 = time.time()
	tokens = tokenize_file(filename)
	t2 = time.time()
	wfm = compute_word_frequencies(tokens)
	t3 = time.time()
	print_word_frequency(wfm)
	t4 = time.time()
	print
	print 'Time taken to tokenize_file            : ' + str(t2-t1) + ' seconds'
	print 'Time taken to compute word_frequencies : ' + str(t3-t2) + ' seconds'
	print 'Time taken to print word_frequencies   : ' + str(t4-t3) + ' seconds'

def partC(filename):
	t1 = time.time()
	tokens = tokenize_file(filename)
	t2 = time.time()
	tgf = compute_3grams_frequencies(tokens)
	t3 = time.time()
	print_ngrams(tgf)
	t4 = time.time()
	print
	print 'Time taken to tokenize_file              : ' + str(t2-t1) + ' seconds'
	print 'Time taken to compute 3grams_frequencies : ' + str(t3-t2) + ' seconds'
	print 'Time taken to print 3grams_frequencies   : ' + str(t4-t3) + ' seconds'

def partD(filename):
	t0 = time.time()
	build_anagrams()
	t1 = time.time()
	tokens = tokenize_file(filename)
	t2 = time.time()
	print_anagrams(detect_anagrams(tokens))
	t3 = time.time()
	print
	print 'Time taken to preprocess and build anagrams on the fly : ' + str(t1-t0) + ' seconds'
	print 'Time taken to tokenize_file                            : ' + str(t2-t1) + ' seconds'
	print 'Time taken to detect and print anagrams                : ' + str(t3-t2) + ' seconds'


def partD_db(filename):
	t1 = time.time()
	tokens = tokenize_file(filename)
	t2 = time.time()
	print_anagrams(detect_anagrams_in_db(tokens))
	t3 = time.time()
	print
	print 'Time taken to tokenize_file              : ' + str(t2-t1) + ' seconds'
	print 'Time taken to detect and print anagrams  : ' + str(t3-t2) + ' seconds'

def parse_input():
	usage = '%prog -f <filename> -p <part>'
	parser = OP(usage)
	parser.add_option('-f', '--filename', dest='filename', help='filename of the text file to be tokenized')
	parser.add_option('-p', '--part', dest='part', help='part of proj 1 to run')
	opt, res = parser.parse_args()
	if not opt.filename or not opt.part:
		parser.print_usage()
		sys.exit()
	return opt, res

if __name__ == '__main__':
	(opt, args) = parse_input()
	if opt.part == 'A':
		partA(opt.filename)
	elif opt.part == 'B':
		partB(opt.filename)
	elif opt.part == 'C':
		partC(opt.filename)
	elif opt.part == 'D':
		partD(opt.filename)
	elif opt.part == 'R':
		partD_db(opt.filename)
	else:
		print 'Invalid input for -p'




