import rpyc
import sys
import os
from rpyc.utils.server import ThreadedServer

data_dir = ''
num_d = 1
class DatanodeServer(rpyc.Service):
	class exposed_Datanode():
		def exposed_put(self,block_uuid,data,dnodes,dest):
			tmp = data_dir + dest
			with open(tmp+str(block_uuid),'w') as f:
				f.write(data)
			'''
			if len(dnodes)>0:
				self.forward(block_uuid,data,dnodes)
			'''
			

		def forward(self,block_uuid,data,dnodes):
			print("8888: forwaring to:")
			print(block_uuid, dnodes)
			d_node=dnodes[0]
			dnodes=dnodes[1:]
			host,port=d_node

			con=rpyc.connect(host,port=port)
			d_node = con.root.Datanode()
			d_node.put(block_uuid,data,dnodes)
		
if __name__ == "__main__":
	m_conn = rpyc.connect('localhost', port = 18812, config={"allow_all_attrs": True})
	data_dir,num_d = m_conn.root.dnode_conf()
	t = ThreadedServer(DatanodeServer, port = 8888)
	t.start()
