/*********************
	  He Cheng
	  4420837
*********************/

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import sys.process._

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.util.Calendar

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.util.Sorting._

import net.sf.samtools._

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import collection.mutable.HashMap

import org.apache.spark.RangePartitioner

object DNASeqAnalyzer 
{
final val MemString = "-Xmx14336m" 
final val RefFileName = "ucsc.hg19.fasta"
final val SnpFileName = "dbsnp_138.hg19.vcf"
final val ExomeFileName = "gcat_set_025.bed"
//////////////////////////////////////////////////////////////////////////////

def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize()
		 
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	// For local mode, include the following two lines
	//conf.setMaster("local[" + config.getNumInstances() + "]")
	//conf.set("spark.cores.max", config.getNumInstances())
	
	// For cluster mode, include the following commented line
	//conf.set("spark.shuffle.blockTransferService", "nio") 
	
	val sc = new SparkContext(conf)
	
	// Rest of the code goes here
	// Read the input file
	val fq1 = sc.textFile("/data/spark/fastq/fastq1.fq")
	val fq2 = sc.textFile("/data/spark/fastq/fastq2.fq")

	// Interleave the contents of the two files
    val fq1Index = fq1.zipWithIndex.mapValues(v=>v/4).map{case(k,v)=>(v,k)}
    val fq2Index = fq2.zipWithIndex.mapValues(v=>v/4).map{case(k,v)=>(v,k)}
    val fqUnion = fq1Index.union(fq2Index)
    val fqGroup = fqUnion.groupByKey.sortByKey(true).flatMap{case(x,y)=>List(y.mkString("\n"))}

	// Write all chunks in parallel
    val numInstances = config.getNumInstances.toInt
	val inputFolder = config.getInputFolder
    //val fqPara = sc.parallelize(fqGroup.collect,numInstances)
    //fqPara.saveAsTextFile(inputFolder)
    val fqPart = fqGroup.repartition(numInstances)
    fqPart.saveAsTextFile(inputFolder)
 }
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
