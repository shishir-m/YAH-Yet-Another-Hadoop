import rpyc
import sys
import os


master = rpyc.connect('localhost',port = 18812, config={'allow_public_attrs': True})
master.root.init()
#connection.root.print()

#OPERATIONS -
ip = 0
while(ip != -1):
	print("\nWelcome to Pydoop. Please choose your operation :\n (1)mkdir\n(2)rmdir")
	ip = input()
	#create directory (mkdir)
	if ip == '1':
		f = input("\nEnter file name:")
		op = master.root.mkdir(f)
		print(op)
	#remove directory (rmdir)
	elif ip == '2':
		f = input("\nEnter the name of the directory to be deleted:")
		op = master.root.rmdir(f)
		print(op)
	elif ip == '-1' :
		break
