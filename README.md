# Table of Contents

1. [Challenge Summary] (README.md#challenge-summary)
2. [Details of Implementation] (README.md#details-of-implementation)
3. [Requirements] (README.md#requirements)

For this coding challenge, I developed tools that could help analyze Venmo’s dataset. Some of the challenges here mimic real world problems.


##Challenge Summary

[Back to Table of Contents] (README.md#table-of-contents)

This challenge requires you to:

- Use Venmo payments that stream in to build a  graph of users and their relationship with one another.

- Calculate the median degree of a vertex in a graph and update this each time a new Venmo payment appears. You will be calculating the median degree across a 60-second sliding window.

The vertices on the graph represent Venmo users and whenever one user pays another user, an edge is formed between the two users.

##Details of implementation

[Back to Table of Contents] (README.md#table-of-contents)

This challenge submission uses python 2.7. The following steps are taken in the main computational engine that processes each transaction as it unrolls from the transaction JSON file.

1. First, the entirety of the JSON file is converted into an efficient dictionary data structure. 
2. The file content is then processed one transaction at a time.
3. Each transaction that comes out of file content is put into a module that maintains 60 second window by determining whether or not the new transaction falls in the 60 second window and removing any old transactions.
4. The output of the module, called sixtysecond_timeframe, include the new sixty second window (called sixtysec_transactions) as well as a list of all added and removed transactions to update the dynamic graph.
5. These outputs are processed by Update_Graph that updates the dictionary-based data structure called Graph_Count which keeps track of the degree of all nodes in the graph.
6. The updated graph_count graph is then processed by compute_median which returns the median to 2 digits significance.
7. Finally, the median is stored in the output.txt

The algorithm is optimized by the use of datetime utilities as well as using as little memory as possible (reusing data structures where possible in loops). This algorithm runs in O(N T + T log T) where N is the total number of transactions, and T is the largest number of transactions within any 60 second window. The sorting of the 60 second window is done by timsort which is used in computing the median. The insertion of each new transaction in a 60 second window takes a total of N T.   

There are numerous unit tests used that are defined in insight_testsuite/run_tests.py and insight_testsuite/tests.py. All unit tests use different types of transactions and preloaded graphs to test the intricate details of different test cases. 

##Requirements
[Back to Table of Contents] (README.md#requirements)

unittest

pytz==2015.6

dateutil

datetime


(C) 2016 Sriram Moparthy
