import sys
import pandas
import networkx as nx
import math

import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions
from graphframes import *


sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)

''' return the simple closure of the graph as a graphframe.'''
	
def simple(g):
	# Extract edges and make a data frame of "flipped" edges
	ed = g.edges
	flip = ed.selectExpr('dst','src').rdd.map(list)
	flip_df = sqlContext.createDataFrame(flip,['src','dst'])

	e = ed.unionAll(flip_df).distinct()
	
	# Combine old and new edges. Distinctify to eliminate multi-edges
	# Filter to eliminate self-loops.
	# A multigraph with loops will be closured to a simple graph
	# If we try to undirect an undirected graph, no harm done
	g1 = GraphFrame(g.vertices,e)
	
	return g1

''' return a data frame of the degree distribution of each edge in 
     the provided graphframe '''

def degreedist(g):
	# Generate a DF with degree,count
	count = g.inDegrees\
		.selectExpr('id as id','inDegree as degree')\
		.groupBy('degree')\
		.count()
	return count

''' read in an edgelist file with lines of the format id1<delim>id2
    and return a corresponding graphframe. If "large" we assume  
    a header row and that delim = " ", otherwise no header and 
    delim = ","'''

def readFile(filename, large, sqlContext=sqlContext):
	lines = sc.textFile(filename)

	if large:
		delim=" "
		# Strip off header row.
		lines = lines.mapPartitionsWithIndex(lambda ind,it: iter(list(it)[1:]) if ind==0 else it)
	else:
		delim=","

	# Extract pairs from input file and convert to data frame matching
	# schema for graphframe edges.
	edgeSchema = StructType([
			StructField("src", IntegerType(), True),
			StructField("dst", IntegerType(), True)])
	
	rdd = lines.map(lambda line: line.split(delim)).map(lambda line: (int(line[0]),int(line[1]))).collect()
	#rdd = lines.map(lambda row: row.split(delim))
	edge_df = sqlContext.createDataFrame(rdd,edgeSchema)
	

	# Extract all endpoints from input file (hence flatmap) and create
	# data frame containing all those node names in schema matching
	# graphframe vertices
	v = edge_df.selectExpr('src as id').unionAll(edge_df.selectExpr('dst as id')).distinct()	
	
	# Create graphframe g from the vertices and edges.	
	g = GraphFrame(v,edge_df) 
	return g


def checkPowerLaw(degDistDF, filename, iterator=None):

	largeK = degDistDF.sort('degree',ascending=False)
	largeK = largeK.selectExpr("degree as degree", "count as freq")
	df = degDistDF.agg({"count" : "sum"})
	n = df.select(df['sum(count)']).collect()[0][0]
	numOfK = int(largeK.count()*0.50)
	largeK = largeK.head(numOfK)
	x = map((lambda row: (row.degree, -math.log(float(row.freq)/n)/math.log(float(row.degree)))), largeK)			
	xs = [c[0] for c in x]
	ys = [c[1] for c in x]
	plt.plot(xs, ys, 'ro')
	plt.title(filename)
	plt.show()
	if iterator is None:
		plt.savefig('myfig.png')
	else:
		name = 'random_graph_' + str(iterator) 
		plt.savefig(name + '.png')


# If you got a file, yo'll parse it.
if len(sys.argv) > 1:
	filename = sys.argv[1]
	if len(sys.argv) > 2 and sys.argv[2]=='large':
		large=True
	else:
		large=False

	print("Processing input file " + filename)
	g = readFile(filename, large)

	print("Original graph has " + str(g.edges.count()) + " directed edges and " + str(g.vertices.count()) + " vertices.")

	g2 = simple(g)
	print("Simple graph has " + str(g2.edges.count()/2) + " undirected edges.")

	distrib = degreedist(g2)
	distrib.show()
	nodecount = g2.vertices.count()
	print("Graph has " + str(nodecount) + " vertices.")

	out = filename.split("/")[-1]
	print("Writing distribution to file " + out + ".csv")
	distrib.toPandas().to_csv(out + ".csv")

	checkPowerLaw(distrib,out)

# Otherwise, generate some random graphs.
else:
	print("Generating random graphs.")
	vschema = StructType([StructField("id", IntegerType())])
	eschema = StructType([StructField("src", IntegerType()),StructField("dst", IntegerType())])

	gnp1 = nx.gnp_random_graph(100, 0.05, seed=1234)
	gnp2 = nx.gnp_random_graph(2000, 0.01, seed=5130303)
	gnm1 = nx.gnm_random_graph(100,1000, seed=27695)
	gnm2 = nx.gnm_random_graph(1000,100000, seed=9999)

	todo = {"gnp1": gnp1, "gnp2": gnp2, "gnm1": gnm1, "gnm2": gnm2}
	iterator = 1
	for gx in todo:
		print("Processing graph " + gx)
		v = sqlContext.createDataFrame(sc.parallelize(todo[gx].nodes()), vschema)
		e = sqlContext.createDataFrame(sc.parallelize(todo[gx].edges()), eschema)
		g = simple(GraphFrame(v,e))
		distrib = degreedist(g)

		print("Writing distribution to file " + gx + ".csv")
		distrib.toPandas().to_csv(gx + ".csv")
		checkPowerLaw(distrib, gx, iterator)
		iterator += 1