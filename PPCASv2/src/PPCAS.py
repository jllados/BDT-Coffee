from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext
import sys
import ctypes
import numpy as np
from numpy.ctypeslib import ndpointer
import time
import math
from operator import add
from pyspark.sql.types import *
from cassandra.cluster import Cluster

def parse_input(file_name):
	name = None
	seq = ""
	count = 0
	seq_list = []
	
	with open(file_name) as f:
		for line in f:
			line = line.strip()
			if(len(line)>0):
				if(line[0]==">"):
					seq_list.append((count,name,seq))
					name=line[1:]
					seq= ""
					count = count + 1
				else: seq=seq+line
		seq_list.append((count,name,seq))
		seq_list.pop(0)
	return seq_list
	
def tc_header(out_name, seq_list):
	file_ = open(out_name + ".lib", 'w')
	file_.write('! TC_LIB_FORMAT_01\n')
	file_.write(str(len(seq_list)) + "\n")
	for i in xrange(len(seq_list)):
		file_.write(seq_list[i][1] + " " + str(len(seq_list[i][2])) + " " + seq_list[i][2] + "\n")
	file_.close()
	
def generate_tasks(n_seq, n_part):
	n_comb = float((float(n_seq) * (float(n_seq) - 1)) / 2)
	max_it = math.ceil(n_comb/n_part)
	it = 0

	tasks = []
	for i in range(0, n_seq):
		for j in range(i + 1, n_seq):
			if(it == 0):
				tasks.append((i, j, int(max_it)))
			
			it +=1
			if(max_it == it):
				n_comb -= max_it
				if(n_comb <= 0): break
				n_part -= 1
				max_it = math.ceil(n_comb/n_part)
				it = 0
	return tasks
	
def main(sc, sqlContext, seq, n_partitions, c_ip, chunk, out_name):
	
	def ProbaMatrix2CL(row):
		n_t1 = row[0]
		n_t2 = row[1]
		max_it = row[2]
		it = 0
		
		libstr = ""
		mapped_list = []
		try:
			for t1 in SEQ_LIST.value[n_t1:]:
				for t2 in SEQ_LIST.value[n_t2:]:
					lib = np.empty((len(t1[2])*len(t2[2]),3), dtype='int32')
					lib = lib.ravel()

					_libraryC = ctypes.CDLL("./PPCAS.so")
					array_1d = np.ctypeslib.ndpointer(ctypes.c_int,flags="C_CONTIGUOUS")
					_libraryC.proba_pair_wise.argtypes = [ctypes.c_char_p, ctypes.c_char_p, array_1d]
					list_n = _libraryC.proba_pair_wise(t1[2], t2[2], lib)
					lib = np.resize(lib,(list_n, 3))
					
					for line in lib:
						mapped_list.append((str(t1[0]-1) + " " + str(line[0].item()/CHUNK.value), str(line[0].item()), str(t2[0]-1), str(line[1].item()), str(line[2].item())))
						mapped_list.append((str(t2[0]-1) + " " + str(line[1].item()/CHUNK.value), str(line[1].item()), str(t1[0]-1), str(line[0].item()), str(line[2].item())))
						
					del lib
					it+=1
					if(it == max_it): raise StopIteration()
				n_t2 = t1[0] + 1
		except StopIteration:
			pass
		return mapped_list

	seq_list = parse_input(file_name)
	
	#SORT
	seq_list = sorted(seq_list, key=lambda constraint: constraint[1])
	for i in range(len(seq_list)):
		seq_list[i] = (i+1,seq_list[i][1],seq_list[i][2])
	
	SEQ_LIST = sc.broadcast(seq_list)
	CHUNK = sc.broadcast(chunk)
	
	if(n_partitions):
		tasks = generate_tasks(len(seq_list), float(n_partitions))
	else:
		tasks = generate_tasks(len(seq_list), float(len(seq_list)))
	rdd_tasks = sc.parallelize(tasks, len(tasks))
	
	rdd_lib = rdd_tasks.flatMap(ProbaMatrix2CL) #StorageLevels
	
	#CASSANDRA CONNECTION
	cluster = Cluster([c_ip], connect_timeout = 10)
	session = cluster.connect()
	#CREATE NEW SCHEMAS
	session.execute("CREATE KEYSPACE if not exists PPCAS WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };")
	session.execute("DROP TABLE IF EXISTS PPCAS." + out_name + ";")
	#session.execute("CREATE TABLE PPCAS." + seq + "_bdtc (key text, s2 int, r2 int, w int, PRIMARY KEY((key), s2, r2));")
	session.execute("CREATE TABLE PPCAS." + out_name + " (key text, r1 int, s2 int, r2 int, w int, PRIMARY KEY((key), r1, s2, r2));")
	
	session.shutdown()
	cluster.shutdown()
			
	#rdd_lib.toDF().selectExpr("_1 as key", "_2 as s2", "_3 as r2", "_4 as w").write.format("org.apache.spark.sql.cassandra").mode('append').options(table="" + seq.lower() + "_bdtc", keyspace="ppcas").save()
	rdd_lib.toDF().selectExpr("_1 as key", "_2 as r1", "_3 as s2", "_4 as r2", "_5 as w").write.format("org.apache.spark.sql.cassandra").mode('append').options(table="" + out_name, keyspace="ppcas").save()

	del seq_list[:]
	
if __name__ == "__main__":
	file_name = sys.argv[1]
	seq = file_name.rpartition('/')[2].split('.')[0]
	n_partitions = int(sys.argv[2])
	c_ip = sys.argv[3]
	chunk = int(sys.argv[4])
	out_name = sys.argv[5]
	
	APP_NAME = "PPCAS_" + seq 
	conf = SparkConf().setAppName(APP_NAME)
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	main(sc, sqlContext, seq, n_partitions, c_ip, chunk, out_name)
