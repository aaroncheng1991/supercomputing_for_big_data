from xml.dom import minidom
import sys
import os
import time

# numInstances and numThreads would be required for cluster mode
doc = minidom.parse("./config.xml")
numInstances = doc.getElementsByTagName("numInstances")[0].firstChild.data
numThreads = doc.getElementsByTagName("numThreads")[0].firstChild.data
	
def run():
	#cmdStr = "$SPARK_HOME/bin/spark-submit " + \
	#"--class \"DNASeqAnalyzer\" --master local[*] --driver-memory 128g dnaseqanalyzer_2.10-1.0.jar"
	cmdStr = "/home/hecheng/spark-1.5.0-bin-hadoop2.6/bin/spark-submit " + \
	"--class \"DNASeqAnalyzer\" --master local[*] --driver-memory 20g /home/hecheng/Assignment3/Part1/target/scala-2.10/dnaseqanalyzer_2.10-1.0.jar"
	
	print cmdStr
	os.system(cmdStr)
	
start_time = time.time()

run()
	
time_in_secs = int(time.time() - start_time)
mins = time_in_secs / 60
secs = time_in_secs % 60

print "{Time taken = " + str(mins) + " mins " + str(secs) + " secs}"
