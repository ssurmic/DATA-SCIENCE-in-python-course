#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 19:57:21 2019

@author: zizhao zhang

mrjob takes a sequence of (label, value) pairs and outputs a collection of 
(label, number of samples, mean, variance) 4 - tuples, in which one 4-tuple 
appeards for each class label in the data, and the mean and variance are
the sample mean and variance. 

"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from functools import reduce
import sys

class MR_summary_stats(MRJob):
    '''
    subclass extending from MRjob
    mrjob scripts for taking stdin to stdout
    '''
    def mapper(self, _, line):
        '''
        first mapper to return the label and the tuple of 
        customized values
        '''
        #get the list of line ele
        line_list = line.strip().split()
        #assign label and ele and ensure the length
        if not line_list or len(line_list) != 2:
            print("txt format invalid")
            #stderr
            sys.exit(1)
        label, value = int(line_list[0]), float(line_list[1])
        # need 1 for count of label
        # output value for summation
        # for the calculation of variance we need X-squared(value**2)
        yield (label, (1, value, value**2))
    def reducer_sum(self, key, value):
        '''
        the reducer to sums up all the elements in the "value" 
        according to the index
        '''
        sum_pair = reduce(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2]), value)
        yield key, sum_pair
    def mapper_format(self, key, value):
        '''
        the final mapper to format the result from previous sums 
        into the desired statistics and format
        Note: when calculating mean use n instead of n-1
        '''    
        #mapping out desired format
        yield key, (value[0], value[1]/value[0], value[2]/value[0] - (value[1]/value[0])**2)
    def steps(self):
        '''
        override steps function to return a list of different
        mappers and reducers
        '''
        return [
            MRStep(mapper = self.mapper,reducer = self.reducer_sum),
            MRStep(mapper = self.mapper_format)
        ]
#from bash run scripts
if __name__ == '__main__':
    MR_summary_stats.run()