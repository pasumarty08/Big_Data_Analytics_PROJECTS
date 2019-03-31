import sys
import time
import networkx as nx
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions
from pyspark.sql.functions import *
from graphframes import *
from copy import deepcopy

sc=SparkContext("local", "degree.py")
sqlContext = SQLContext(sc)


def articulations(g, usegraphframe=False):
	
	articulations = []
	# Get the starting count of connected components
	print g.vertices.count()
	count = 1
	# Default version sparkifies the connected components process 
	# and serializes node iteration.
	if usegraphframe:
		# Get vertex list for serial iteration
		vertices = g.vertices.collect()
		# For each vertex, generate a new graphframe missing that vertex
		# and calculate connected component count. Then append count to
		# the output
		result = g.connectedComponents()
		originalNumOfComponents = g.connectedComponents().agg(countDistinct("component").alias("num")).collect()[0].num
		for row in g.vertices.collect():
			# here we are computing the number of components in the graph after we remove the current node of iteration 
			numOfComponents = GraphFrame(g.vertices.filter("id != '"+row.id+"'"),g.edges).connectedComponents().agg(countDistinct("component").alias("num")).collect()[0].num
			if numOfComponents > originalNumOfComponents:
				articulations.append((row.id,1))
			else:
				articulations.append((row.id,0))
			count = count + 1

		articulationDataFrame = sqlContext.createDataFrame(ard,["id","articulation"])
		return articulationDataFrame
	# Non-default version sparkifies node iteration and uses networkx 
	# for connected components count.
	else:
		gx = nx.Graph()
		gx.add_nodes_from(g.vertices.map(lambda x : x.id).collect())
		gx.add_edges_from(g.edges.map(lambda x : (x.src, x.dst)).collect())

		baseCount = nx.number_connected_components(gx)

		def components(n):
			g = deepcopy(gx)
			g.remove_node(n)
			return nx.number_connected_components(g)

		return sqlContext.createDataFrame(g.vertices.map(lambda x: (x.id, 1 if components(x.id) > baseCount else 0)), ['id','articulation'])

filename = sys.argv[1]
lines = sc.textFile(filename)

pairs = lines.map(lambda s: s.split(","))
e = sqlContext.createDataFrame(pairs,['src','dst'])
e = e.unionAll(e.selectExpr('src as dst','dst as src')).distinct() # Ensure undirectedness 	

# Extract all endpoints from input file and make a single column frame.
v = e.selectExpr('src as id').unionAll(e.selectExpr('dst as id')).distinct()	

# Create graphframe from the vertices and edges.
g = GraphFrame(v,e)

#Runtime approximately 5 minutes
print("---------------------------")
print("Processing graph using Spark iteration over nodes and serial (networkx) connectedness calculations")
init = time.time()
df = articulations(g, False)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)
print("Writing distribution to file articulations_out.csv")
df.toPandas().to_csv("articulations_out.csv")
print("---------------------------")

#Runtime for below is more than 2 hours
print("Processing graph using serial iteration over nodes and GraphFrame connectedness calculations")
init = time.time()
df = articulations(g, True)
print("Execution time: %s seconds" % (time.time() - init))
print("Articulation points:")
df.filter('articulation = 1').show(truncate=False)