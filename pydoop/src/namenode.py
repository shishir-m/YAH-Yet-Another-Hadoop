import json
import sys
import argparse
import rpyc
import math
import os


if __name__=="__main__":
	#setting values from config -
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
	
	block_size = config[0]
	path_to_datanodes = config[1]
	path_to_namenodes = config[2]
	rep_f = config[3]
	num_d = config[4]
	datanode_size = config[5]
	sync_period = config[6]
	datanode_log_path = config[7]
	namenode_log_path = config[8]
	namenode_checkpoints = config[9]
	fs_path = config[10]
	dfs_setup_config = config[11]
