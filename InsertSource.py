#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 11:29:42 2017

@author: vgupta
"""

from cassandra.cqlengine import connection
ip =''#insert host connection of db

connection.setup(hosts = [ip], default_keyspace = "test", protocol_version=3)

from fncassandra.Source import Source

fname = "List/sourceName.csv"
with open(fname) as f_in:
    for sourceName in f_in:
        sourceName = sourceName.rstrip('\n')
        Source.create(name=str(sourceName),score=0.0)

