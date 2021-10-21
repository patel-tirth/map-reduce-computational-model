package MRJobs.TypesOfLogMessages
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
import java.time.format.DateTimeFormatter
import java.util.Date


class TypesOfLogMessagesMapper extends Mapper[Object, Text, Text, IntWritable]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val pattern = config.getString("randomLogGenerator.Pattern").r
  val timeFrom = config.getString("randomLogGenerator.TimeFrom")
  val timeTo = config.getString("randomLogGenerator.TimeTo")
//  val datetime = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
  val format = new java.text.SimpleDateFormat("HH:mm:ss.SSS")
  // parse time to run map on predefined time intervals
  val startTime = format.parse(timeFrom)  // parse the desired start time from the config file
  val endTime = format.parse(timeTo)   // parse the desired end time from the config file

  def checkDateFormat(str: String)  =
     Try[Date](format.parse(str)) match {
       case Success(testDate) => true
       case Failure(exception) => false
     }

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    logger.info("inside map function of TypesOfLogMessagesMapper")
    val one = new IntWritable(1)
    val word = new Text()

    val pattern = config.getString("randomLogGenerator.Pattern").r
    val itr = new StringTokenizer(value.toString)
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken())
    // first check for time interval, if the string is of formate time, then proceed
    if(checkDateFormat(word.toString)) {
      // if time is between predefined time then proceed to check for info, debug, warn or error
      if (format.parse(word.toString).before(endTime) && format.parse(word.toString).after(startTime)) {
        var t1 = word.toString
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        if (word.toString.equals("INFO")) {
          // if the word is info, then check if the log message mathches our regex pattern
          // if it does, then write the time, error message and the log message for output
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          pattern.findFirstMatchIn(word.toString).map(e => {
            //          word.set("TOTAL INFO WITH MATCHED REGEX")
            var i = t1 + " INFO " + word
            context.write(new Text(i), one)
          })
        } else if (word.toString.equals("WARN")) {
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          pattern.findFirstMatchIn(word.toString).map(e => {
            //          word.set("TOTAL WARN WITH MATCHED REGEX")
            var w = t1 + " WARN " + word
            context.write(new Text(w), one)
          })

        } else if (word.toString.equals("DEBUG")) {
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          pattern.findFirstMatchIn(word.toString).map(e => {
            //          word.set("TOTAL DEBUG WITH MATCHED REGEX")
            var d = t1 + " DEBUG " + word
            context.write(new Text(d), one)
          })
        } else if (word.toString.equals("ERROR")) {
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          pattern.findFirstMatchIn(word.toString).map(e => {
            //          word.set("TOTAL ERROR WITH MATCHED REGEX")
            var e = t1 + " ERROR " + word
            context.write(new Text(e), one)
          })
        }


      }
    }
    }


    }

}
