#!/usr/bin/python
'''
Unit tests for the different modules in the Insight Data Challenge. These tests take in different graph structures derived from Venmo Transaction JSON datasets and compute the median within 60 second windows.
'''

__version__ = '0.0'
__author__ = 'Sriram Moparthy'
__email__ = 'moparthy26@gmail.com'

from os import path
import sys
sys.path.append(path.abspath('../'))
sys.path.append(path.abspath('../src'))

import unittest
import json
import time

from src.__main__ import *


class SixtySecondWindowTest(unittest.TestCase):
    '''
    This unit test ensures that the sixty second window is maintained.
    '''
    def test_SixtySec_TimeFrame(self,
        Transaction,
        SixtySec_Transactions,
        SixtySec_Transactions_result):
        '''
        This asserts that the window after a Transaction put in SixtySec_Transactions will results in a new window SixtySec_Transactions_result.

        @param Transaction: New transaction that has rolled in
        @param SixtySec_Transactions: The previous sixty second window of transactions
        @param SixtySex_Transactions_result: The true sixty second window after Transaction has entered
        @rtype Boolean
        '''
        SixtySec_Transactions_new,Updated_Transactions,Removed_Transactions = sixtysec_timeframe(Transaction,SixtySec_Transactions)
        SixtySec_Transactions_new = list(SixtySec_Transactions_new)

        self.maxDiff = None

        try:
            self.assertEqual(
            SixtySec_Transactions_new,
            SixtySec_Transactions_result)
            return True
        except Exception, e:
            print str(e)
            return False

    def runTest(self):
        '''
        Runs multiple cases of tests.

        @rtype Boolean
        '''

        count = 0

        # Define test data sets and results
        SixtySec_Transactions = [
                {'created_time': '2016-03-28T23:28:19Z',
                    'target': 'E',
                    'actor': 'Y'},
                {'created_time': '2016-03-28T23:29:10Z',
                    'target': 'E',
                    'actor': 'Z'},
                {'created_time': '2016-03-28T23:29:11Z',
                    'target': 'E',
                    'actor': 'T'},
                {'created_time': '2016-03-28T23:29:13Z',
                    'target': 'L',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:14Z',
                    'target': 'C',
                    'actor': 'D'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'A',
                    'actor': 'B'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'B',
                    'actor': 'C'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'}
            ]

        SixtySec_Transactions_case1 = [
                {'created_time': '2016-03-28T23:28:19Z',
                    'target': 'E',
                    'actor': 'Y'},
                {'created_time': '2016-03-28T23:29:10Z',
                    'target': 'E',
                    'actor': 'Z'},
                {'created_time': '2016-03-28T23:29:11Z',
                    'target': 'E',
                    'actor': 'T'},
                {'created_time': '2016-03-28T23:29:13Z',
                    'target': 'L',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:14Z',
                    'target': 'C',
                    'actor': 'D'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'A',
                    'actor': 'B'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'B',
                    'actor': 'C'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'}
            ]

        SixtySec_Transactions_case2 = [
                {'created_time': '2016-03-28T23:28:19Z',
                    'target': 'E',
                    'actor': 'Y'},
                {'created_time': '2016-03-28T23:28:58Z',
                    'target': 'G',
                    'actor': '2'},
                {'created_time': '2016-03-28T23:29:10Z',
                    'target': 'E',
                    'actor': 'Z'},
                {'created_time': '2016-03-28T23:29:11Z',
                    'target': 'E',
                    'actor': 'T'},
                {'created_time': '2016-03-28T23:29:13Z',
                    'target': 'L',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:14Z',
                    'target': 'C',
                    'actor': 'D'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'A',
                    'actor': 'B'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'B',
                    'actor': 'C'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'}
            ]

        SixtySec_Transactions_case3 = [
                {'created_time': '2016-03-28T23:28:19Z',
                    'target': 'E',
                    'actor': 'Y'},
                {'created_time': '2016-03-28T23:29:10Z',
                    'target': 'E',
                    'actor': 'Z'},
                {'created_time': '2016-03-28T23:29:11Z',
                    'target': 'E',
                    'actor': 'T'},
                {'created_time': '2016-03-28T23:29:13Z',
                    'target': 'L',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:14Z',
                    'target': 'C',
                    'actor': 'D'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'A',
                    'actor': 'B'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'B',
                    'actor': 'C'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'},
                {'created_time': '2016-03-28T23:29:18Z',
                    'target': 'G',
                    'actor': '3'}
            ]

        SixtySec_Transactions_case4 = [
                {'created_time': '2016-03-28T23:29:10Z',
                    'target': 'E',
                    'actor': 'Z'},
                {'created_time': '2016-03-28T23:29:11Z',
                    'target': 'E',
                    'actor': 'T'},
                {'created_time': '2016-03-28T23:29:13Z',
                    'target': 'L',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:14Z',
                    'target': 'C',
                    'actor': 'D'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'A',
                    'actor': 'B'},
                {'created_time': '2016-03-28T23:29:15Z',
                    'target': 'B',
                    'actor': 'C'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'},
                {'created_time': '2016-03-28T23:29:20Z',
                    'target': 'G',
                    'actor': '4'}
            ]

        SixtySec_Transactions_case5 = [
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'D',
                    'actor': 'G'},
                {'created_time': '2016-03-28T23:29:16Z',
                    'target': 'G',
                    'actor': 'E'},
                {'created_time': '2016-03-28T23:29:17Z',
                    'target': 'E',
                    'actor': 'F'},
                {'created_time': '2016-03-28T23:30:15Z',
                    'target': 'G',
                    'actor': '5'}
            ]

        # Case 1: Transaction is too old; Window should not change
        Transaction ={
            'created_time': '1800-03-25T20:29:10Z',
            'target': 'G',
            'actor': '1'}

        case1 = self.test_SixtySec_TimeFrame(
            Transaction,list(SixtySec_Transactions),
            SixtySec_Transactions_case1)

        if(case1):
            count += 1

        # Case 2: Transaction is old but fits within 60 second window
        Transaction = {
            'created_time': '2016-03-28T23:28:58Z',
            'target': 'G',
            'actor': '2'}

        case2 = self.test_SixtySec_TimeFrame(
            Transaction,list(SixtySec_Transactions),
            SixtySec_Transactions_case2)

        # Print results
        print 'Case 2: ' + str(case2)

        # Case 3: Transaction is new and no old ones are thrown away
        Transaction = {
            'created_time': '2016-03-28T23:29:18Z',
            'target': 'G',
            'actor': '3'}

        case3 = self.test_SixtySec_TimeFrame(
            Transaction,list(SixtySec_Transactions),
            SixtySec_Transactions_case3)

        # Print results
        print 'Case 3: ' + str(case3)

        # Case 4: Transaction is new and the oldest transaction
        # is thrown away
        Transaction = {
            'created_time': '2016-03-28T23:29:20Z',
            'target': 'G',
            'actor': '4'}

        case4 = self.test_SixtySec_TimeFrame(
            Transaction,list(SixtySec_Transactions),
            SixtySec_Transactions_case4)

        # Print results
        print 'Case 4: ' + str(case4)

        # Case 5: Transaction is new and multiple old transactions
        # are thrown away.
        Transaction = {
            'created_time': '2016-03-28T23:30:15Z',
            'target': 'G',
            'actor': '5'}


        case5 = self.test_SixtySec_TimeFrame(
            Transaction,list(SixtySec_Transactions),
            SixtySec_Transactions_case5)

        # Print results
        print 'Case 5: ' + str(case5)

        return case1 and case2 and case3 and case4 and case5


class UpdateGraphTest(unittest.TestCase):
    '''
    This unit test checks that the graph data structure, which is in the form of a dictionary containing all the nodes along with the degree as its value.
    '''
    def test_UpdateGraph(self, Graph_Count, Updated_Transactions, Removed_Transactions, Graph_Count_result):
        '''
        Tests the update step and checks that the new degree count stored in the dictionary Graph_Count matches Graph_Count_result after updated and removed transactions are taken into accoutn (adding/removing edges)

        @param Updated_Transactions: New transaction to be added to graph
        @param Removed_Transactions: Old edges/transactions removed from graph
        @param Graph_Count: Data structure of graph before update
        @param Graph_Count_result: True graph degree count after transactions are taken into account
        @rtype Boolean
        '''

        try:
            self.assertEqual(
                update_graph(Graph_Count,
                    Updated_Transactions,
                    Removed_Transactions),
                Graph_Count_result)
            return True
        except Exception,e:
            print str(e)
            return False

    def runTest(self):
        '''
        Runs different cases of updated/removed transactions.

        @rtype Boolean
        '''

        # Define test data sets and results
        # A,B,D -> E |
        #  |-> F  <-|
        #      |
        #     |-> G -> Y -> Z
        Graph_Count = {
            'a':2,
            'b':2,
            'd':2,
            'e':4,
            'f':5,
            'g':2,
            'y':2,
            'z':1
        }

        Graph_Count_case1 = {
            'a':2,
            'b':2,
            'd':2,
            'e':4,
            'f':5,
            'g':2,
            'y':2,
            'z':1
        }

        Graph_Count_case2 = {
            'a':2,
            'b':2,
            'd':3,
            'e':4,
            'f':5,
            'g':2,
            'y':2,
            'z':1,
            'c':1
        }

        Graph_Count_case3 = {
            'a':2,
            'b':2,
            'd':3,
            'e':3,
            'f':3,
            'y':1,
            'z':1,
            'c':1,
        }

        Graph_Count_case4 = {
            'a':1,
            'b':2,
            'd':3,
            'e':2,
            'f':3,
            'c':1
        }

        # Case 1: No new updated transaction and no new
        # removed transaction
        Updated_Transactions = []
        Removed_Transactions = []
        case1 = self.test_UpdateGraph(
            dict(Graph_Count),
            Updated_Transactions,
            Removed_Transactions,
            Graph_Count_case1)

        print 'Case 1: ' + str(case1)

        # Case 2: One updated transaction and no removed transaction
        Updated_Transactions = [
            {'created_time': '2016-03-28T23:29:16Z',
                'target': 'D',
                'actor': 'C'}
        ]
        Removed_Transactions = []
        case2 = self.test_UpdateGraph(
            dict(Graph_Count),
            Updated_Transactions,
            Removed_Transactions,
            Graph_Count_case2)

        print 'Case 2: ' + str(case2)

        # Case 3: One updated transaction and three removed transaction
        Updated_Transactions = [
            {'created_time': '2016-03-28T23:29:16Z',
                'target': 'D',
                'actor': 'C'}
        ]
        Removed_Transactions = [
            {'created_time': '2016-03-28T23:27:16Z',
                'target': 'F',
                'actor': 'E'},
            {'created_time': '2016-03-28T23:27:18Z',
                'target': 'G',
                'actor': 'F'},
            {'created_time': '2016-03-28T23:27:19Z',
                'target': 'Y',
                'actor': 'G'}
        ]
        case3 = self.test_UpdateGraph(
            dict(Graph_Count),
            Updated_Transactions,
            Removed_Transactions,
            Graph_Count_case3)

        print 'Case 3: ' + str(case3)

        # Case 4: One updated transaction and five removed transaction
        Updated_Transactions = [
            {'created_time': '2016-03-28T23:29:16Z',
                'target': 'D',
                'actor': 'C'}
        ]
        Removed_Transactions = [
            {'created_time': '2016-03-28T23:27:16Z',
                'target': 'F',
                'actor': 'E'},
            {'created_time': '2016-03-28T23:27:18Z',
                'target': 'G',
                'actor': 'F'},
            {'created_time': '2016-03-28T23:27:19Z',
                'target': 'Y',
                'actor': 'G'},
            {'created_time': '2016-03-28T23:27:19Z',
                'target': 'Z',
                'actor': 'Y'},
            {'created_time': '2016-03-28T23:27:20Z',
                'target': 'E',
                'actor': 'A'}
        ]
        case4 = self.test_UpdateGraph(
            dict(Graph_Count),
            Updated_Transactions,
            Removed_Transactions,
            Graph_Count_case4)

        print 'Case 4: ' + str(case4)

        return case1 and case2 and case3 and case4


class ComputeMedianTest(unittest.TestCase):
    '''
    Unit test to compute median of graph (Graph_Count data structure.)
    '''
    def test_ComputeMedian(self, Graph_Count, Median_result):
        '''
        Checks that compute_median computes the median as expected in Median_result.

        @param Graph_Count: Graph datastructure that keeps track of all degrees of all nodes
        @param Median_result: Expected median result from graph
        @rtype Boolean
        '''
        try:
            self.assertEqual(
            compute_median(
                Graph_Count),
            Median_result)
            return True
        except Exception,e:
            print str(e)
            return False

    def runTest(self):
        '''
        Runs the test cases

        @rtype Boolean
        '''

        # Define test data sets and results
        Graph_Count_case1 = {
            'A':1,
            'B':1,
            'D':1,
            'E':4,
            'F':5,
            'G':2,
            'Y':2,
            'Z':1,
        }
        Median_result_case1 = 1.5

        Graph_Count_case2 = {
            'A':1,
            'B':1,
            'D':2,
            'E':4,
            'F':5,
            'G':2,
            'Y':2,
            'Z':1,
            'C':1,
        }
        Median_result_case2 = 2.0

        Graph_Count_case3 = {
            'A':1,
            'B':1,
            'D':2,
            'E':3,
            'F':3,
            'Y':1,
            'Z':1,
            'C':1,
        }
        Median_result_case3 = 1.0

        Graph_Count_case4 = {
            'B':1,
            'D':2,
            'E':2,
            'F':3,
            'C':1,
        }
        Median_result_case4 = 2.0

        # Case 1: No new updated transaction and no new
        # removed transaction
        case1 = self.test_ComputeMedian(
            Graph_Count_case1,
            Median_result_case1)

        # Case 2: One updated transaction and no removed transaction
        case2 = self.test_ComputeMedian(
            Graph_Count_case2,
            Median_result_case2)

        # Case 3: One updated transaction and three removed transaction
        case3 = self.test_ComputeMedian(
            Graph_Count_case3,
            Median_result_case3)

        # Case 4: One updated transaction and five removed transaction
        case4 = self.test_ComputeMedian(
            Graph_Count_case4,
            Median_result_case4)

        print 'Case 1: ' + str(case1)
        print 'Case 2: ' + str(case2)
        print 'Case 3: ' + str(case3)
        print 'Case 4: ' + str(case4)

        return case1 and case2 and case3 and case4

class ChallengeTest(unittest.TestCase):
    '''
    Unit test to test the entire engine of computing rolling medians.
    '''
    def test(self,output_path,true_output_path):
        '''
        Main test script to make sure the output text files match. In some cases, new lines may cause an issue.

        @param Output path to file output by the main engine
        @param Output path to hard coded manually created file with true rolling medians
        @rtype Boolean
        '''
        try:
            output = open(output_path,'r').readlines()
            true_output = open(true_output_path,'r').readlines()
            self.assertEqual(output,true_output)
            return True
        except Exception,e:
            print str(e)
            return False

    def runTest(self):
        '''
        Runs the test for 2 cases stored in tests/ folder. This folder structure follows that shown on the github page for the challenge.

        @rtype Boolean
        '''

        for case_id in [1,2,3,4]:
            try:
                input_path = 'tests/test-%i-venmo-trans/venmo_input/test-%i-venmo-trans.txt'%(case_id,case_id)
                output_path = 'tests/test-%i-venmo-trans/venmo_output/output.txt'%(case_id)
                true_output_path = 'tests/test-%i-venmo-trans/venmo_output/true_output.txt'%(case_id)
                start = time.clock()
                run_challenge(input_path,output_path)
                end = time.clock()
                print 'Processing took: %s sec'%(end-start)
                print self.test(output_path,true_output_path)
            except Exception, e:
                print str(e)
                return False
