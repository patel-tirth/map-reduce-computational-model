package MRJobs.SortedTimeIntervals
import Generation.{LogMsgSimulator, RandomStringGenerator}
import HelperUtils.Parameters.*
import HelperUtils.{CreateLogger, Parameters}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
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
import java.time.format.DateTimeFormatter
import java.util.Date
// job 2 mapper
//compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected
// regex pattern string instances
class SortedTimeIntervalMatchedMapper extends Mapper[LongWritable, Text, IntWritable, Text]{
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val format = new java.text.SimpleDateFormat("HH:mm:ss.SSS")

  def checkDateFormat(str: String)  =
    Try[Date](format.parse(str)) match {
      case Success(testDate) => true
      case Failure(exception) => false
    }
//  val timeL = List.empty[String]

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, IntWritable, Text]#Context): Unit = {
    logger.info("inside map function of SortedTimeIntervalMatchedMapper")
    val one = new IntWritable(1)
    val word = new Text()
//    var c = 1
    val pattern = config.getString("randomLogGenerator.Pattern").r

    val itr = new StringTokenizer(value.toString)

    while(itr.hasMoreTokens()){
      word.set(itr.nextToken())

      if(checkDateFormat(word.toString)){
        var t1 = word.toString
//        val time = format.parse(word.toString)
        word.set(itr.nextToken())
        word.set(itr.nextToken())
        if(word.toString.equals("ERROR")){
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          word.set(itr.nextToken())
          pattern.findFirstMatchIn(word.toString).map(m => {
//             val i = t1
              context.write(one,new Text(t1+" ERROR " + word.toString))
            })
//          c+=1s
        }
      }

    }

  }
}
