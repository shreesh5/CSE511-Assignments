#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
from threading import Thread 

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeSort(i, InputTable, SortingColumnName, start, stop, openconnection):

    cursor = openconnection.cursor()

    #Create partition table
    prefix = 'range_sort_part{}'.format(i)
    cursor.execute('CREATE TABLE {0} (like {1} including all)'.format(prefix,InputTable))
    openconnection.commit()

    #Insert into partition table
    insertquery = 'INSERT INTO {0} SELECT * FROM {1} WHERE {2} ORDER BY {3}'
    if start is None:
        whereclause = '{0} <= {1}'.format(SortingColumnName, stop)
    else:
        whereclause = '{0} > {1} AND {0} <= {2}'.format(SortingColumnName, start, stop)
    insertquery = insertquery.format(prefix, InputTable, whereclause, SortingColumnName)

    cursor.execute(insertquery)
    openconnection.commit()

def ParallelSort(InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.

    nthreads = 5
    cursor = openconnection.cursor()

    #selecting max and min to create partitions
    maxminquery = 'SELECT MAX({0}), MIN({0}) FROM {1}'.format(SortingColumnName, InputTable)
    cursor.execute(maxminquery)
    maxval, minval = cursor.fetchone()
    stepsize = float(maxval-minval)/nthreads

    partitions = []
    for i in range(1, nthreads+1):
        partitions.append([minval + (i - 1) * stepsize, minval + i *stepsize])
    partitions[0][0] = None

    argslist = []
    for i, (start, stop) in enumerate(partitions):
        argslist.append((i, InputTable, SortingColumnName, start, stop, openconnection))

    threads = []
    for args in argslist:
        thread = Thread(target=RangeSort, args=args)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    cursor.execute('CREATE TABLE {0} (like {1} including all)'.format(OutputTable, InputTable))
    openconnection.commit()
    insertquery = 'INSERT INTO {0} SELECT * FROM {1}'

    for i in range(len(partitions)):
        prefix = 'range_sort_part{0}'.format(i)
        cursor.execute(insertquery.format(OutputTable, prefix))
        openconnection.commit()

def RangeJoin(i, start, stop, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    cursor = openconnection.cursor()

    # Create partition table
    prefix1 = '{0}_join_part{1}'.format(InputTable1,i)
    prefix2 = '{0}_join_part{1}'.format(InputTable2,i)

    cursor.execute('CREATE TABLE {0} (like {1} including all)'.format(prefix1, InputTable1))
    openconnection.commit()

    cursor.execute('CREATE TABLE {0} (like {1} including all)'.format(prefix2, InputTable2))
    openconnection.commit()

    # Insert into partition table
    insertquery = 'INSERT INTO {0} SELECT * FROM {1} WHERE {2}'
    if start is None:
        whereclause1 = '{0} <= {1}'.format(Table1JoinColumn, stop)
        whereclause2 = '{0} <= {1}'.format(Table2JoinColumn, stop)
    else:
        whereclause1 = '{0} > {1} AND {0} <= {2}'.format(Table1JoinColumn, start, stop)
        whereclause2 = '{0} > {1} AND {0} <= {2}'.format(Table2JoinColumn, start, stop)

    insertquery1 = insertquery.format(prefix1, InputTable1, whereclause1)
    insertquery2 = insertquery.format(prefix2, InputTable2, whereclause2)

    cursor.execute(insertquery1)
    openconnection.commit()

    cursor.execute(insertquery2)
    openconnection.commit()

    insertquery_out = 'INSERT INTO {0} SELECT * FROM {1} INNER JOIN {2} ON {1}.{3}={2}.{4}'
    insertquery_out = insertquery_out.format(OutputTable, prefix1, prefix2, Table1JoinColumn, Table2JoinColumn)
    cursor.execute(insertquery_out)
    openconnection.commit()


def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.

    nthreads = 5
    cursor = openconnection.cursor()

    createtab = '''CREATE TABLE {0} AS SELECT * FROM {1} INNER JOIN {2} ON {1}.{3}={2}.{4} LIMIT 0'''
    createtab = createtab.format(OutputTable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn)
    cursor.execute(createtab)
    openconnection.commit()

    #Create partitions
    query = 'SELECT MIN({0}), MAX({0}) FROM {1}'.format(Table1JoinColumn, InputTable1)
    cursor.execute(query)
    minval1, maxval1 = cursor.fetchone()

    query = 'SELECT MIN({0}), MAX({0}) FROM {1}'.format(Table2JoinColumn, InputTable2)
    cursor.execute(query)
    minval2, maxval2 = cursor.fetchone()

    minval = min(minval1, minval2)
    maxval = max(maxval1, maxval2)
    stepsize = float(maxval - minval) / nthreads

    partitions = []
    for i in range(1, nthreads+1):
        partitions.append([minval + (i - 1) * stepsize, minval + i *stepsize])
    partitions[0][0] = None

    argslist = []
    for i, (start, stop) in enumerate(partitions):
        argslist.append((i, start, stop, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection))

    threads = []
    for args in argslist:
        thread = Thread(target=RangeJoin, args=args)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()