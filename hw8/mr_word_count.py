#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 16 19:57:21 2019

###############################################
This file contains the class extends from mrjob
in the purpose of returning the according 
frequency and the word given a text as input
###############################################

@author: zizhao zhang
"""

###import library###
from mrjob.job import MRJob
import re

proper_words = re.compile(r'[\w\']+')

class Text_frequency_count (MRJob):
    # Mapper to read line, yield that word and a counter of 1 per line
    def mapper(self, _,line):
        line = line.strip()
        for word in proper_words.findall(line):
            yield (word.lower(), 1)
    # Optional combiner for the optimization to count words
    # def combiner (self, word, count):
       # yield (word, sum(count))
    # Reduce over all nodes to get word count in a format of [words, count].
    def reducer(self, word, count):
        yield (word, sum(count))
#to run in bash
if __name__ == '__main__':
    Text_frequency_count.run()
