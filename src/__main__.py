#!/usr/bin/python
''' This challenge requires the creation of a dynamic connection graph
    and computes the median degree over 60 second windows of active
    Venmo transactions.
'''

__version__ = '0.0'
__author__ = 'Sriram Moparthy'
__email__ = 'moparthy26@gmail.com'

import sys

import json
import pytz
import time
from datetime import datetime
from dateutil import parser


def venmo_trans(filename):
    '''
    The Venmo_Trans function first imports a text file that contains Venmo
    transactions in JSON formation.  This text file will be read using the
    readlines function.

    The Venmo_Trans function then converts the unicode strings into regular
    strings.

    The Venmo_Trans function returns File Content - a list of dictionaries
    of Venmo Transactions.
    '''
    file_content_unicode = []
    f = open(filename, 'r').readlines()
    for line in f:
        file_content_unicode.append(json.loads(line))
    file_content = []
    for item in file_content_unicode:
        file_content.append(dict([(str(k), str(v)) for k, v in item.items()]))
    return file_content


def sixtysec_timeframe(transaction,sixtysec_transactions):

    '''
    The SixtySec_TimeFrame function takes takes two inputs: Transaction and
    SixtySec_Transactions.

    If the sixtysec_transaction list is empty, then add the first transaction
    to this list and the updated_transactions list.  For the next transaction,
    check if it is within 60 sec of all transactions in the
    sixtysec_transaction list.  If it is, then append it to the
    updated_transactions list.  If any transaction is not within 60 seconds
    of the maximum timestamp in the sixtysec_transactions list, then remove
    that transaction from the sixtysec_transactions list and append it to the
    removed_transaction list.

    This function returns the following: SixtySec_Transactions,
    Updated_Transactions, and Removed_Transaction.

    '''
    # Initialize outputs
    # Initialize new time window
    updated_sixty_transactions = []
    updated_transactions = []
    removed_transactions = []

    # If not thrown away:
    # Check if SixtySec_Transactions has any transactions to begin with
    if(len(sixtysec_transactions) == 0):
        # Only 1 transaction
        updated_sixty_transactions.append(transaction)
        updated_transactions.append(transaction)
    else:
        # Get the time stamp of Transaction
        transaction_time = transaction['created_time']

        # Convert time stamp to datetime object
        transaction_time = parser.parse(transaction_time)

        # Get last transaction in 60 sec window
        last_transaction = sixtysec_transactions[-1]

        # Get the timestamp from the last transaction in the 60 sec window
        last_transaction_time = parser.parse(last_transaction['created_time'])

        # Check if Transaction should be thrown away by making sure new Transaction happens after last one
        time_difference = transaction_time - last_transaction_time

        # THROW IT AWAY
        if(time_difference.total_seconds() < -60):
            return sixtysec_transactions, updated_transactions, removed_transactions
        else:
            # Set last transaction time
            last_transaction_time = max(last_transaction_time,transaction_time)

            # Iterate over all SixtySec window and throw >60sec old transactions out
            for old_transaction in sixtysec_transactions:
                time_difference = last_transaction_time - parser.parse(old_transaction['created_time'])
                # If >60 sec
                if(time_difference.seconds >= 60):
                    # Add it to the removed transactions
                    removed_transactions.append(old_transaction)
                else:
                    # Add to our new 60 second window
                    updated_sixty_transactions.append(old_transaction)
            updated_sixty_transactions.append(transaction)
            # Sort SixtySec to always be chronological in O(N lg N) time
            updated_sixty_transactions.sort(key=lambda trans: (parser.parse(trans['created_time'])-datetime(1970,1,1,tzinfo=pytz.utc)).total_seconds())
            updated_transactions.append(transaction)

    return updated_sixty_transactions, updated_transactions, removed_transactions

def update_graph(graph_count,updated_transactions,removed_transactions):

    """
    The update_graph function takes in three inputs: graph count,
    updated transactions, and removed transactions.

    In this function, we first get the target and actor for each
    transaction.

    The update_graph function returns graph_count - a dictionary
    of actors/target and the degree.
    """
    # For each updated transaction
    for transaction in updated_transactions:
        # Get target
        target = transaction['target'].lower()

        # Check if target is empty
        if(target == ''):
            continue

        # Check if target is in dictionary
        if(target not in graph_count.keys()):
            # Add it to the dictionary
            graph_count[target] = 0

        # Get actor
        actor = transaction['actor'].lower()

        # Check if actor is empty
        if(actor == ''):
            continue

        # Check if actor is in dictionary
        if(actor not in graph_count.keys()):
            graph_count[actor] = 0

        # Add 1 to both actor and target in graph count
        graph_count[target] = graph_count[target] + 1
        graph_count[actor] = graph_count[actor] + 1

    # For each removed transaction
    for transaction in removed_transactions:
        # Get target
        target = transaction['target'].lower()

        # Check if target is empty
        if(target == ''):
            continue

        # Check if target is in dictionary
        if(target not in graph_count.keys()):
            # Add it to the dictionary
            graph_count[target] = 0

        # Get actor
        actor = transaction['actor'].lower()

        # Check if actor is empty
        if(actor == ''):
            continue

        # Check if actor is in dictionary
        if(actor not in graph_count.keys()):
            # Add it to the dictionary
            graph_count[actor] = 0

        # Subtract 1 to both actor and target in graph count
        graph_count[target] = graph_count[target] - 1
        graph_count[actor] = graph_count[actor] - 1

        # Check if it becomes 0, pop it
        if(graph_count[target] == 0):
            graph_count.pop(target)
        if(graph_count[actor] == 0):
            graph_count.pop(actor)

    return graph_count

def compute_median(graph_count):
    counts = graph_count.values()
    counts.sort()
    if(len(counts)%2 == 0):
        median = (counts[len(counts)/2-1] + counts[len(counts)/2])/2.0
    else:
        median = counts[int(len(counts)/2)]
    return median

def run_challenge(input_path,output_path):
    # Load data
    file_content = venmo_trans(input_path)

    # Initialize the 60 second window that will hold venmo transactions within 60 sec of the last transaction
    sixtysec_transactions = []

    # Initialize the Graph Count dictionary
    graph_count = {}

    # Initialize output file
    output = open(output_path,'w')

    # Pop the data and feed into engine that keeps count of median
    for transaction in file_content:
        # Maintain the 60 second window
        sixtysec_transactions_new, updated_transactions, removed_transactions = sixtysec_timeframe(transaction, sixtysec_transactions)

        # Update the graph
        graph_count = update_graph(graph_count,updated_transactions,removed_transactions)

        # Compute the median
        SixtySec_Median = compute_median(graph_count)

        # Write to output.txt
        output.write('%.2f'%(SixtySec_Median)+'\n')

    # Close the file
    output.close()


if __name__ == "__main__":
    input_fn = sys.argv[1]
    ouput_fn = sys.argv[2]
    start = time.clock()
    run_challenge(
        input_fn,
        ouput_fn
        )
    end = time.clock()
    print 'Processing took: %s sec'%(end-start)


