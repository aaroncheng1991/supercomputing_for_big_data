import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import TutorialHelper._

object Tutorial {
  def main(args: Array[String]) {
    
    // Checkpoint directory
    val checkpointDir = TutorialHelper.getCheckpointDirectory()

    // Configure Twitter credentials
    val apiKey = "PPLefYZ2uZuuIHskv7tvbvqhM"
    val apiSecret = "FFJydSxeNwGnj5SDKhlx6TyYifmgGWM0WhwlReao04yE5gdhUy"
    val accessToken = "255336987-wwN7z02iTOXBqKpy3VQcmLAPjQ9bodCpaizp9AV4"
    val accessTokenSecret = "OnvyD2pBu2MQzmH4vo2lLUsRSb7oLbHCuBXKFJXhrpYNa"
    TutorialHelper.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    // Your code goes here
    val ssc = new StreamingContext(new SparkConf(), Seconds(1))
    val tweets = TwitterUtils.createStream(ssc, None)
// 2.2
//  val statuses = tweets.map(status => status.getText())
//	val counts = statuses.map(tag => (tag, 1))
//    	                 .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(1))
//    counts.print()

//2.3
//original
	val counts = tweets.filter(_.isRetweet).map (status => 
               	status.getText()) 
             	.countByValueAndWindow(Seconds(60), Seconds(1))

//modified   cannot compile
//	val counts = tweets.filter(_.isRetweet).map (status => 
//               	status.getText()) 
//             	.reduceByValueAndWindow(Seconds(60), Seconds(1))


	val sortedCounts = counts.map { case(tag, count) => (count, tag) }
                         .transform(rdd => rdd.sortByKey(false))
//2.4
	sortedCounts.foreach(rdd =>
    println("\nTop 5 tweets within the 60 second window:\n" + rdd.take(5).mkString("\n")))


//    statuses.print()
//	val words = statuses.flatMap(status => status.split(" "))
//	val hashtags = words.filter(word => word.startsWith("#"))
//	hashtags.print()
//	val counts = hashtags.map(tag => (tag, 1))
//    	                 .reduceByKeyAndWindow(_ + _, _ - _, Seconds(60 * 5), Seconds(1))
//    counts.print()
//	val sortedCounts = counts.map { case(tag, count) => (count, tag) }
//    	                     .transform(rdd => rdd.sortByKey(false))
//	sortedCounts.foreach(rdd =>
//  		println("\nTop 10 hashtags:\n" + rdd.take(10).mkString("\n")))
    ssc.checkpoint(checkpointDir)
    ssc.start()
    ssc.awaitTermination()
  }
}

