import rpyc
import sys
import os


connection = rpyc.connect('localhost',port = 18812, config={'allow_public_attrs': True})
connection.root.print()
