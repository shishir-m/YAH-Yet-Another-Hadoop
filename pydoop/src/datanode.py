import rpyc
import sys
import os
from rpyc.utils.server import ThreadedServer

class DatanodeServer(rpyc.Service):
	class exposed_Datanode():
		
if __name__ == "__main__":
	connection = rpyc.connect('localhost', port = 18812, config={"allow_all_attrs": True})

	t = ThreadedServer(DatanodeServer, port = 8888)
	t.start()
