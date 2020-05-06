#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    result = []
    cursor = openconnection.cursor()

    partquery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'''.format(ratingMinValue, ratingMaxValue)
    cursor.execute(partquery)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]

    rangeselectquery = '''SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'''

    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0,'RangeRatingsPart{}'.format(partition))
            result.append(res)

    rrcountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    rrselectquery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating>={1} and rating<={2};'''

    for i in xrange(0,rrparts):
        cursor.execute(rrselectquery.format(i, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)

    writeToFile('RangeQueryOut.txt', result)




def PointQuery(ratingsTableName, ratingValue, openconnection):
    result = []
    cursor = openconnection.cursor()

    partquery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={0};'''.format(ratingValue)
    cursor.execute(partquery)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]

    rangeselectquery = '''SELECT * FROM rangeratingspart{0} WHERE rating={1};'''

    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RangeRatingsPart{}'.format(partition))
            result.append(res)

    rrcountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''

    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    rrselectquery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating={1};'''

    for i in xrange(0, rrparts):
        cursor.execute(rrselectquery.format(i, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)

    writeToFile('PointQueryOut.txt', result)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
