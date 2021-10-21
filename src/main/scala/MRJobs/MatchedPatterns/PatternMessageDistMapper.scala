package MRJobs.MatchedPatterns

import Generation.{LogMsgSimulator, RandomStringGenerator}
import HelperUtils.Parameters.*
import HelperUtils.{CreateLogger, Parameters}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.slf4j.{Logger, LoggerFactory}

import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, duration}
import scala.util.{Failure, Success, Try}
class PatternMessageDistMapper extends Mapper[Object, Text, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val pattern = config.getString("randomLogGenerator.Pattern").r

//  val numPattern = pattern.r

  //  def getPatternFromConfig(parameters: Parameters.type ): Unit ={
  //    logger.info("getting the pattern from config file for mapping")
  //    val config = ConfigFactory.load("application")
  //    val pattern = config.getConfig("randomLogGenerator.Pattern")
  //    return pattern
  ////    return Parameters.Pattern
  //  }
  
  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    logger.info("inside map function of pattern message distribution")
//    val pattern = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}".r
    val one = new IntWritable(1)
    val word = new Text()
    val itr = new StringTokenizer(value.toString)
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken())
      if(word.toString.equals("INFO")) {
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        pattern.findFirstMatchIn(word.toString).map(m => {
          word.set("Total INFO")
          context.write(word, one)
        })
      } else if (word.toString.equals("DEBUG")){
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        pattern.findFirstMatchIn(word.toString).map(m => {
          word.set("Total DEBUG")
          context.write(word, one)
        })
      } else if (word.toString.equals("WARN")){
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        pattern.findFirstMatchIn(word.toString).map(m => {
          word.set("Total WARN")
          context.write(word, one)
        })
      } else if (word.toString.equals("ERROR")){
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        pattern.findFirstMatchIn(word.toString).map(m => {
          word.set("Total ERROR")
          context.write(word, one)
        })
      }

    }
  }
}
