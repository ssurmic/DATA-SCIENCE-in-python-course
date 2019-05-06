#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 22:45:29 2019

@author: zizhao zhang

PySpark Job to take the described input and produces a list of all the
triangles in the network, one per line

allocation set up part is referenced from http://www-personal.umich.edu/~klevin/teaching/Winter2019/STATS507/ps_wordcount.py

"""

from pyspark import SparkConf, SparkContext
import sys
import itertools 



# This script takes two arguments, an input and output
if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <in> <out>')
    sys.exit(1)
inputlocation = sys.argv[1]
outputlocation = sys.argv[2]

# Set up the configuration and job context
conf = SparkConf().setAppName("CountTriangles")
sc = SparkContext(conf=conf)

# Read in the dataset 
data = sc.textFile(inputlocation)

#define a mapper function
def triplets_mapper(line):
    '''
    mapper for transforming input to output key value pairs
    where keys are tuples of friends and value is a count
    
    '''
    line_list = line.split()
    #store myself and friends tuple of len(2)
    myself_list = []
    myself_list.append(int(line_list[0]))
    two_friends = itertools.combinations([int(i) for i in line_list[1:]],2)
    #return a list of tuple of triangle as key, 1 as value
    return [ (tuple(sorted(myself_list + list(i))), 1) for i in two_friends ]


# Map the data into key-value paris
data_mapped = data.flatMap(triplets_mapper)
# Sum up how many times this appears and then filter for the ones more than 2
# For their to be a triangle, tuple must 
data_mapped = data_mapped.reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= 2)
res = data_mapped.keys().sortBy(lambda x: x)
res.map(lambda x: x[0])
res.saveAsTextFile(outputlocation)
sc.stop()# Let Spark know that the job is done.