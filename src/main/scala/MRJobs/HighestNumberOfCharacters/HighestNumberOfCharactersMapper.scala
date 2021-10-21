package MRJobs.HighestNumberOfCharacters
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

import java.text.SimpleDateFormat
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, duration}
import scala.util.{Failure, Success, Try}

//produce the number of characters in each log message for each log message type that contain
// the highest number of characters in the detected instances of the designated regex pattern.
class HighestNumberOfCharactersMapper extends Mapper[Object, Text, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
//    val one = new IntWritable(1)
    val word = new Text()
    val itr = new StringTokenizer(value.toString)
//    var maxCountInfo = 0
//    var maxCountDebug = 0
//    var maxCountWarn = 0
//    var maxCountError = 0
    while(itr.hasMoreTokens()){
      if(word.toString.equals("INFO")){
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        context.write(new Text("INFO highest character count"),new IntWritable(word.toString.toCharArray.length))
//          if(word.toString.toCharArray.length > maxCountInfo)
//            maxCountInfo = word.toString.toCharArray.length
      } else if (word.toString.equals("DEBUG")){
//        if(word.toString.toCharArray.length > maxCountDebug)
//          maxCountDebug = word.toString.toCharArray.length
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        context.write(new Text("DEBUG highest character count"),new IntWritable(word.toString.toCharArray.length))
      } else if (word.toString.equals("WARN")){
//        if(word.toString.toCharArray.length > maxCountWarn)
//          maxCountWarn = word.toString.toCharArray.length
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        context.write(new Text("WARN highest character count"),new IntWritable(word.toString.toCharArray.length))
      } else if (word.toString.equals("ERROR")){
//        if(word.toString.toCharArray.length > maxCountError)
//          maxCountError = word.toString.toCharArray.length
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        context.write(new Text("ERROR highest character count"),new IntWritable(word.toString.toCharArray.length))
      }
    }

  }
}
