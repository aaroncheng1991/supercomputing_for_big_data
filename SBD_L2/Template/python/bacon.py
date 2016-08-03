# bacon.py
from pyspark import SparkConf, SparkContext
import time
import sys

KEVIN_BACON	=	'Bacon, Kevin (I)'	# This is how Kevin Bacon's name appears in the actors.list file

cores = sys.argv[1].strip()
input_file = sys.argv[2].strip()
						
start_time = time.time()

conf = (SparkConf()
         .setMaster("local[" + cores + "]")
         .setAppName("Kevin Bacon app")
		 .set("spark.cores.max", cores)
		 )
sc = SparkContext(conf = conf)

print "Number of cores: " + cores
print "Input file: " + input_file

# Main code here #############################################################
# ...
##############################################################################
		
# Write to "actors.txt", the following:
#	Total number of actors
#	Total number of movies (and TV shows etc)
#	Total number of actors at distances 1 to 6 (Each distance information on new line)
#	The name of actors at distance 6 sorted alphabetically (ascending order), with each actor's name on new line

time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "{Time taken = " + str(mins) + " mins " + str(secs) + " secs}"		
	