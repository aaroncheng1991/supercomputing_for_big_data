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

import collection.mutable.ArrayBuffer

object DNASeqAnalyzer 
{
final val MemString = "-Xmx14336m" 
final val RefFileName = "ucsc.hg19.fasta"
final val SnpFileName = "dbsnp_138.hg19.vcf"
final val ExomeFileName = "gcat_set_025.bed"
//////////////////////////////////////////////////////////////////////////////
def bwaRun (x: String, config: Configuration) : 
	Array[(Int, SAMRecord)] = 
{
	// Get the path of folders
	val refFolder = config.getRefFolder
	val numOfThreads = config.getNumThreads
	val toolsFolder = config.getToolsFolder
	val inFileName = config.getInputFolder + x
	val outFileName = config.getTmpFolder + x + ".sam"

	// Create the command string (bwa mem...)and then execute it using the Scala's process package.
	Seq(toolsFolder+"bwa","mem",refFolder+RefFileName,"-p","-t",numOfThreads,inFileName) #> new File(outFileName)!
	
	val bwaKeyValues = new BWAKeyValues(outFileName)
	bwaKeyValues.parseSam()
	val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()
	
	// Delete the temporary files
	Files.deleteIfExists(Paths.get(outFileName))
	
	return kvPairs
}

def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], config: Configuration) : ChromosomeRange = 
{
	val header = new SAMFileHeader()
	header.setSequenceDictionary(config.getDict())
	val outHeader = header.clone()
	outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
	val factory = new SAMFileWriterFactory();
	val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));
	
	val r = new ChromosomeRange()
	val input = new SAMRecordIterator(samRecordsSorted, header, r)
	while(input.hasNext()) 
	{
		val sam = input.next()
		writer.addAlignment(sam);
	}
	writer.close();
	
	return r
}

def variantCall (chrRegion: Int, samRecordsSorted: Array[SAMRecord], config: Configuration): 
	Array[(Integer, (Integer, String))] = 
{	
	val tmpFolder = config.getTmpFolder
	val toolsFolder = config.getToolsFolder
	val refFolder = config.getRefFolder
	val numOfThreads = config.getNumThreads
	
	// SAM records should be sorted by this point
	val regP1Bam = tmpFolder + "region" + chrRegion + "-p1.bam"
	val chrRange = writeToBAM(regP1Bam, samRecordsSorted, config)
	
	// Picard preprocessing
	val regP2Bam = tmpFolder + "region" + chrRegion + "-p2.bam"	
	Seq("java",MemString,"-jar",toolsFolder + "CleanSam.jar","INPUT=" + regP1Bam,"OUTPUT=" + regP2Bam)!

	val regP3Bam = tmpFolder + "region" + chrRegion + "-p3.bam"
	val regP3MetTxt = tmpFolder + "region" + chrRegion + "-p3-metrics.txt"
	Seq("java",MemString,"-jar",toolsFolder+"MarkDuplicates.jar","INPUT=" + regP2Bam,"OUTPUT=" + regP3Bam,"METRICS_FILE=" + regP3MetTxt)!	

	val regBam = tmpFolder + "region" + chrRegion + ".bam"
	Seq("java",MemString,"-jar",toolsFolder+"AddOrReplaceReadGroups.jar","INPUT=" + regP3Bam,"OUTPUT=" + regBam,
	    "RGID=GROUP1","RGLB=LIB1","RGPL=ILLUMINA","RGPU=UNITI","RGSM=SAMPLE1")!

	Seq("java",MemString,"-jar",toolsFolder + "BuildBamIndex.jar","INPUT=" + regBam)!

	// Delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt	
	Files.deleteIfExists(Paths.get(regP1Bam))
	Files.deleteIfExists(Paths.get(regP2Bam))
	Files.deleteIfExists(Paths.get(regP3Bam))
	Files.deleteIfExists(Paths.get(regP3MetTxt))

	// Make region file 
	val tmpBed = tmpFolder + "tmp" + chrRegion + ".bed"
	val bedBed = tmpFolder + "bed" + chrRegion + ".bed"
	val tmpBedFile = new File(tmpBed)
	chrRange.writeToBedRegionFile(tmpBedFile.getAbsolutePath())
	Seq(toolsFolder + "bedtools","intersect","-a",refFolder + ExomeFileName,"-b",tmpBed,"-header") #> new File(bedBed)!

	// Delete tmpFolder/tmpX.bed
	Files.deleteIfExists(Paths.get(tmpBed))

	// Indel Realignment 
	val GATKJar = toolsFolder + "GenomeAnalysisTK.jar"
	val regInt = tmpFolder + "region" + chrRegion + ".intervals"
	val refFile = refFolder + RefFileName
	Seq("java",MemString,"-jar",GATKJar,"-T","RealignerTargetCreator","-nt",numOfThreads,"-R",refFile,"-I",regBam,"-o",regInt,"-L",bedBed)!

	val reg2Bam = tmpFolder + "region" + chrRegion + "-2.bam"
	Seq("java",MemString,"-jar",GATKJar,"-T","IndelRealigner","-R",refFile,"-I",regBam, "-targetIntervals",regInt,"-o",reg2Bam,"-L",bedBed)!

	// Delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
	Files.deleteIfExists(Paths.get(regBam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + ".bai"))
	Files.deleteIfExists(Paths.get(regInt))

	
	// Base quality recalibration 
	val regTab = tmpFolder + "region" + chrRegion + ".table"
	Seq("java",MemString,"-jar",GATKJar,"-T","BaseRecalibrator","-nct",numOfThreads,"-R",refFile,"-I",reg2Bam,"-o",regTab,"-L",bedBed,
		"--disable_auto_index_creation_and_locking_when_reading_rods","-knownSites",refFolder + SnpFileName)!
 
	val reg3Bam = tmpFolder + "region" + chrRegion + "-3.bam"
	Seq("java",MemString,"-jar",GATKJar,"-T","PrintReads","-R",refFile, "-I",reg2Bam,"-o",reg3Bam,"-BQSR",regTab,"-L",bedBed)!

	// Delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table
	Files.deleteIfExists(Paths.get(reg2Bam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + "-2.bai"))
	Files.deleteIfExists(Paths.get(regTab))	


	// Haplotype -> Uses the region bed file
	val regVcf = tmpFolder + "region" + chrRegion + ".vcf"
	Seq("java",MemString,"-jar",GATKJar,"-T","HaplotypeCaller","-nct",numOfThreads,"-R",refFile,"-I",reg3Bam,"-o",regVcf,"-stand_call_conf","30.0",
		"-stand_emit_conf","30.0","-L",bedBed,"--no_cmdline_in_header","--disable_auto_index_creation_and_locking_when_reading_rods")!

	// Delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed
	Files.deleteIfExists(Paths.get(reg3Bam))
	Files.deleteIfExists(Paths.get(tmpFolder + "region" + chrRegion + "-3.bai"))
	Files.deleteIfExists(Paths.get(bedBed)) 
	
	// Return the content of the vcf file produced by the haplotype caller in the form of <Chromsome number, <Chromosome Position, line>>
    val vcfLine = Source.fromFile(regVcf).getLines.filter(_.startsWith("chr"))

    val vcfAB = new ArrayBuffer[(Integer, (Integer, String))]()
    for (line <- vcfLine) {
      val lineArray = line.split("\t")
      val chrNum = lineArray(0).substring(3) match {
            case "X" => 23
            case "Y" => 24
            case _ => lineArray(0).substring(3).toInt
          }
     
      val pos = lineArray(1).toInt
      vcfAB += new Pair(chrNum,new Pair(pos,line))
    }
    return vcfAB.toArray
}

// define load balancing function
def balanceLoad(SAMRecNum:Int, regNum:Int): Array[Int] = {
	val regIndex = new Array[Int](SAMRecNum)
	val lowLoad = SAMRecNum/regNum
	val highLoad = lowLoad + 1
	val highLoadRegNum = SAMRecNum%regNum
	var regNo = 0
	while (regNo < regNum){
		if (regNo < highLoadRegNum){
			for (i <- regNo*highLoad to ((regNo+1)*highLoad - 1)){
				regIndex(i) = regNo
			}
		}
		else{
			for (i <- (highLoadRegNum*highLoad + (regNo-highLoadRegNum)*lowLoad) to (highLoadRegNum*highLoad + (regNo-highLoadRegNum+1)*lowLoad - 1)){
				regIndex(i) = regNo
			}
		}
		regNo = regNo + 1 
	}
	return regIndex
}

def main(args: Array[String]) 
{
	val config = new Configuration()
	config.initialize()
		 
	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	// For local mode, include the following two lines
	conf.setMaster("local[" + config.getNumInstances() + "]")
	conf.set("spark.cores.max", config.getNumInstances())
	conf.set("spark.ui.port", "9090")
	
	// For cluster mode, include the following commented line
	//conf.set("spark.shuffle.blockTransferService", "nio") 
	
	val sc = new SparkContext(conf)
	
	// Rest of the code goes here
	// Create BWA tasks
	val numInstances = config.getNumInstances.toInt
	val inputFolder = config.getInputFolder
	val chunkDir = new File(inputFolder)	
	val chunkNames = chunkDir.listFiles.filter(_.isFile).map(_.getName).filter(_.startsWith("part")) 
	val interChunks = sc.parallelize(chunkNames,numInstances)
	val configBroadcast = sc.broadcast(config)
	val SAMRecords = interChunks.mapPartitions{iter=>iter.map(x=>bwaRun(x,configBroadcast.value))}

	// Load balancing
	val SAMIndex = SAMRecords.flatMap{x=>x}.sortByKey().zipWithIndex.map{case(k,v)=>(v,k)}
	val SAMNum = SAMIndex.count.toInt
	val SAMBalance = balanceLoad(SAMNum,numInstances)

	// <chromosome region, SAM Record>
	val regionSAM = SAMIndex.partitionBy(new RangePartitioner(numInstances,SAMIndex)).mapPartitions{iter=>iter.map(x=>(SAMBalance(x._1.toInt),x._2._2))}	

	// <Integer, <Integer, String>>
	val intIntStr = regionSAM.groupByKey.mapPartitions{iter=>iter.flatMap(x=>variantCall(x._1,x._2.toArray.sortWith(_.compare(_)<0),configBroadcast.value))}	   
	
	// Sort the lines by chromosome numbers, and for each chromosome, further sort them by the position of the read.
	val intIntStrSort = intIntStr.groupByKey.mapValues{v=>v.toArray.sortWith(_._1 < _._1)}.sortByKey()

	// Produce the combined vcf file
	val vcfLines = intIntStrSort.flatMapValues(v=>v).map(x => x._2._2).collect
	val outputFolder = config.getOutputFolder
	val fw = new FileWriter(new File(outputFolder + "result.vcf"))
	fw.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tSAMPLE1\n")
	for (l <- vcfLines){
		fw.write(l + "\n")
	}
	fw.close
}
//////////////////////////////////////////////////////////////////////////////
} // End of Class definition
