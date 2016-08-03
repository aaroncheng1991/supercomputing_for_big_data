/* Bacon.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat


object Bacon 
{
	final val KevinBacon = "Bacon, Kevin (I)"	// This is how Kevin Bacon's name appears in the actors.list file
	
	def main(args: Array[String]) 
	{
		val cores = args(0)
		val inputFile = args(1)
		
		val conf = new SparkConf().setAppName("Kevin Bacon app")
		conf.setMaster("local[" + cores + "]")
		conf.set("spark.cores.max", cores)
		val sc = new SparkContext(conf)
		
		println("Number of cores: " + args(0))
		println("Input file: " + inputFile)
		
		var t0 = System.currentTimeMillis

		// Main code here ////////////////////////////////////////////////////
		// ...
		//////////////////////////////////////////////////////////////////////

		// Initialization
		// 1. <actor,List<movie>> 
                val hadoopConf = new Configuration()
                hadoopConf.set("textinputformat.record.delimiter","\n\n")
		val path = "/home/aaron/Template/scala/actors.list"
		val stringActorMovies = sc.newAPIHadoopFile(path, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf).map{case(_,movie) => movie.toString}
		val listActorMovies = stringActorMovies.map(_.split("\t+",2).toList)

		//2. <actor,movie>
		val actorMovie = listActorMovies.flatMap(line=>line(1).split("\t+").toList.map(x=>
		    (line(0),x))).map{case(k,v)=>(k,v.split("  ").toList.head)}

		// 3. <actor, actor> 
		val movieActor = actorMovie.map{ case(k, v) => (v, k)}
		val actorActor = movieActor.join(movieActor).map{case(k,v)=>(v._1, v._2)}.filter{case(v1,v2)=>v1!=v2}.distinct()
		
		// 4. <actor,List<collaboratingActors>>
		val actorListActor = actorActor.groupByKey().map{case(actor,listActor) => (actor,listActor.toSet.toList)}

		// 5. <actor,distance>
		val actorDistance = listActorMovies.filter{l=>l(0).nonEmpty}.map{l=>if(l(0).contains(
		    "Bacon, Kevin (I)")) (l(0),0.0) else 
		    (l(0),Double.PositiveInfinity)}


		// Analysis
		// first iteration
		// 1. <actor,<distance,List<collaboratingActor>>>
		val actorDistanceListActor = actorDistance.join(actorListActor)

		//2. <collaboratingActor,distance+1>
		val firstDistance = actorDistanceListActor.flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)

		//3. <collaboratingActor,minDistance>
		val firstMinDistance = actorDistance.join(firstDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}


		// second iteration
		val secondDistance = firstMinDistance.join(actorListActor).flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)
		val secondMinDistance = firstMinDistance.join(secondDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}

		// third iteration
		val thirdDistance = secondMinDistance.join(actorListActor).flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)
		val thirdMinDistance = secondMinDistance.join(thirdDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}

		// forth iteration
		val forthDistance = thirdMinDistance.join(actorListActor).flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)
		val forthMinDistance = thirdMinDistance.join(forthDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}

		// fifth iteration
		val fifthDistance = forthMinDistance.join(actorListActor).flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)
		val fifthMinDistance = forthMinDistance.join(fifthDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}

		// sixth iteration
		val sixthDistance = fifthMinDistance.join(actorListActor).flatMap{x=> x._2._2.map(y=>(y, x._2._1 +1.0))}.reduceByKey((m,n)=>if(m>n) n else m)
		val sixthMinDistance = fifthMinDistance.join(sixthDistance).map{case(x,(y,z))=>if(y < z) (x,y) else (x,z)}

		// Write to "actors.txt", the following:
		//	Total number of actors
		//	Total number of movies (and TV shows etc)
		//	Total number of actors at distances 1 to 6 (Each distance information on new line)
		//	The name of actors at distance 6 sorted alphabetically (ascending order), with each actor's name on new line	
		val actorNum = stringActorMovies.count
		val movieNum = movieActor.groupByKey.count
		val oneDisNum = sixthMinDistance.filter{case(k,v)=> v==1.0}.count 		
		val twoDisNum = sixthMinDistance.filter{case(k,v)=> v==2.0}.count 
		val threeDisNum = sixthMinDistance.filter{case(k,v)=> v==3.0}.count 
		val fourDisNum = sixthMinDistance.filter{case(k,v)=> v==4.0}.count 
		val fiveDisNum = sixthMinDistance.filter{case(k,v)=> v==5.0}.count 
		val sixDisNum = sixthMinDistance.filter{case(k,v)=> v==6.0}.count 

		val sixDisActor = sixthMinDistance.filter{case(k,v)=> v==6.0}.sortByKey(true).map{case(k,v)=> ("",k)}

                val result = sc.makeRDD(Seq(("Total number of actors",actorNum),
        	("Total number of movies (and TV shows etc)", movieNum),
        	("Total number of actors at distance 1", oneDisNum),
        	("Total number of actors at distance 2", twoDisNum),
        	("Total number of actors at distance 3", threeDisNum),
        	("Total number of actors at distance 4", fourDisNum),
        	("Total number of actors at distance 5", fiveDisNum),
        	("Total number of actors at distance 6", sixDisNum),
        	("The name of actors at distance 6", ""))
        	++ sixDisActor.collect.toSeq)

                result.saveAsTextFile("/home/aaron/Template/scala/actors.txt")

		val et = (System.currentTimeMillis - t0) / 1000
		val mins = et / 60
		val secs = et % 60
		println( "{Time taken = %d mins %d secs}".format(mins, secs) )
	} 
}