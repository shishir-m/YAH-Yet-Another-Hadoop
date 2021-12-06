import json
import sys
import argparse
import rpyc #remote procedure calls
from rpyc.utils.server import ThreadedServer
import math
import os.path
import datetime
from collections import defaultdict



class NamenodeServer(rpyc.Service):
	fs_path = ""
	file_table = defaultdict(list)
	block_size = 0
	clients = 1
	def on_connect(self, conn):
		dt = datetime.datetime.now()
		
		print(f"\nClient connected on {dt}")
	def on_disconnect(self, conn):
		dt = datetime.datetime.now()
		self.clients -= 1
		print(f"Client Disconnected on {dt}\n")
		t.close()
	
	def exposed_init(self):
		self.file_table = defaultdict(list)		#absolute path (root directory)
		self.file_table[self.fs_path] = []
		print("init",self.file_table)
	
	def exposed_display(self):
		tmp = self.file_table
		return(self.file_table)	
	
	def exposed_mkdir(self,dir):
		dir_name = dir.split("/")
		flag = 1
		#dir = self.fs_path + dir
		if len(dir_name) > 1:
			string = ''
			flag = 0
			for i in dir_name[:-1]:
				string = i + "/"
		if flag == 0:
			parent = self.fs_path + string
		elif flag == 1:
			parent = self.fs_path
		#print(parent)
		
		if(self.dir_exists(parent) == False):
			return "Parent directory does not exist"
		if(self.dir_exists(self.fs_path + dir_name[0] + "/") == True and len(dir_name) == 1):
			return "Directory already exist"
		if(dir_name[0] not in self.file_table[self.fs_path]):
			self.file_table[self.fs_path].append(dir_name[0])
			#print("entry into file table:",self.file_table)
			self.file_table[self.fs_path + dir_name[0] + "/"] = []
		if len(dir_name) > 1:
			for i in range(len(dir_name) - 1):
			
				tmp_s = ''
				for j in dir_name[:i+1]:
					tmp_s = j + "/"
					#print("tmp_s",tmp_s)
				self.file_table[self.fs_path + tmp_s].append(dir_name[i+1])
				self.file_table[self.fs_path + tmp_s + dir_name[i+1] + "/"] = []
		#self.file_table[dir].append('$')
		print(self.file_table)
		return "\n	----Created new Directory----"
	
	def exposed_rmdir(self,dir):
		dir_name = dir.split("/")
		rem_dir = ''
		string = ''
		for i in dir_name[:-1]:
			string = i + "/"
		if(len(dir_name) > 1):
			rem_dir = self.fs_path + string + dir_name[-1] + "/"
		else:
			rem_dir = self.fs_path + dir_name[0] + "/"
		print(rem_dir)
		parent = self.fs_path + string
			
		if(self.dir_exists(parent) == False):
			return "\nParent directory does not exist"
		elif(dir_name[-1] not in self.file_table[parent]):
			return "\n Directory does not exist"
		if(len(self.file_table[rem_dir]) >= 1):
			return "Directory not empty"
		self.file_table[parent].remove(dir_name[-1])
		del self.file_table[rem_dir]
		print(self.file_table)
		return "\n	----Deleted Directory----"
	def dir_exists(self,file):
		if(file in self.file_table):
			return 1
			
		

if __name__=="__main__":
	#getting values from config.json file -
	parser = argparse.ArgumentParser()
	parser.add_argument('--config', required=False)
	args = parser.parse_args()
	subname = args.config
	if(subname):
		f = open(subname,)
	else:
		f = open("config.json",)

	data = json.load(f)
	if(len(data) !=12):
		print("incorrect config file")
		sys.exit()
	config = []
	for i in data:
		config.append(data[i])
	
	NamenodeServer.block_size = config[0]
	NamenodeServer.path_to_datanodes = config[1]
	NamenodeServer.path_to_namenodes = config[2]
	NamenodeServer.rep_f = config[3]
	NamenodeServer.num_d = config[4]
	NamenodeServer.datanode_size = config[5]
	NamenodeServer.sync_period = config[6]
	NamenodeServer.datanode_log_path = config[7]
	NamenodeServer.namenode_log_path = config[8]
	NamenodeServer.namenode_checkpoints = config[9]
	NamenodeServer.fs_path = config[10]
	NamenodeServer.dfs_setup_config = config[11]
	NamenodeServer.file_table = {}
	
	
	#starting up namenode service
	t = ThreadedServer(NamenodeServer, port=18812)
	t.start()
	
	
	
	
