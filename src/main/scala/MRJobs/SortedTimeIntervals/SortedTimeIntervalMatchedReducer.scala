package MRJobs.SortedTimeIntervals
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import java.lang.Iterable
import java.lang
import java.util.StringTokenizer
import scala.collection.JavaConverters.*
// job 2 reducer

//compute time intervals sorted in the descending order that contained most log messages of the type ERROR with injected
// regex pattern string instances

class SortedTimeIntervalMatchedReducer extends Reducer[IntWritable,Text,Text,IntWritable]{
  override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
//    var sum = values.asScala.foldLeft(0)(_ + _.get)
    values.forEach(v =>
        context.write(v,key)
    )

  }
}
