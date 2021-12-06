import rpyc
import sys
import os


master = rpyc.connect('localhost',port = 18812, config={'allow_public_attrs': True})
master.root.init()

def send_to_datanode(block_uuid,data,nodes,dest):
	main_dnode = nodes[0]
	host,port=main_dnode
	conn=rpyc.connect(host,port=port)
	d_node = conn.root.Datanode()
	d_node.put(block_uuid,data,nodes,dest)

def put(source,dest):
	size = os.path.getsize(source)
	blocks = master.root.write(source,dest,size)
	with open(source) as f:
		for b in blocks:
			data = f.read(master.root.get_block_size())	#reading block_size amt each time
			block_uuid=b[0]
			dnodes = [master.root.get_storage()[_] for _ in b[1]]
			send_to_datanode(block_uuid,data,dnodes,dest)
			#m = rpyc.connect('localhost',port = 18812, config={'allow_public_attrs': True})
			master.root.mk(dest,str(block_uuid))
	
#OPERATIONS -
ip = 0
while(ip != -1):
	print("\nWelcome to Pydoop. Please choose your operation :\n (1)mkdir\n(2)rmdir\n(3)Display directory structure(ls)\n(4)PUT\n(5)GET\n(-1)EXIT")
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
	elif ip == '3':
		op = master.root.ls()
		for i in op:
			print(i,"\t",op[i])	#f"{i}\t{op[i]}\n"
	elif ip == '4':
		source = input("Enter the source file:")
		dest = input("\n Enter the destination directory:")
		put(source,dest)
		print("\n	----SUCCESSFUL----")
	elif ip == '5':
		
	elif ip == '-1' :
		break
