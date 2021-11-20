import json
import sys
import argparse
import rpyc #remote procedure calls
from rpyc.utils.server import ThreadedServer
import math
import os
import datetime



class NamenodeServer(rpyc.Service):
	block_size = 0
	clients = 1
	def on_connect(self, conn):
		dt = datetime.datetime.now()
		
		print(f"\nConnected on {dt}")
	def on_disconnect(self, conn):
		dt = datetime.datetime.now()
		self.clients -= 1
		print(f"\nDisconnected on {dt}")
	'''
	def init(self,config):
		
		print(self.block_size)
	'''
	def exposed_print(self):
		print(self.block_size)

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
	
	
	#starting up namenode service
	t = ThreadedServer(NamenodeServer, port=18812)
	t.start()
	if NamenodeServer.clients == 0:
		t.stop()
	
	
	
