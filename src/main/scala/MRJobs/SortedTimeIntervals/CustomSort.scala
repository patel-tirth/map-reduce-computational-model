package MRJobs.SortedTimeIntervals

import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.slf4j.{Logger, LoggerFactory}

import java.util.Date
import scala.util.{Failure, Success, Try}

// class to sort  in descending order
// keys are sorted before passing to reducer
class CustomSort extends WritableComparator(classOf[IntWritable],true){

//  val format = new java.text.SimpleDateFormat("HH:mm:ss.SSS")
val logger: Logger = LoggerFactory.getLogger(this.getClass)
  override def compare(a: WritableComparable[_], b: WritableComparable[_]) : Int = {
    val a1 = a.isInstanceOf[IntWritable]
    val b1 = b.isInstanceOf[IntWritable]
    logger.info("a1 "+a1)
    logger.info("b1 "+b1)
    -1 * a1.compareTo(b1)

  }
}
