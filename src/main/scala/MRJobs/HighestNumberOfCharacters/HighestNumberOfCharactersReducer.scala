package MRJobs.HighestNumberOfCharacters
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import java.lang.Iterable
import java.util.StringTokenizer
import scala.collection.JavaConverters.*

//def max(list: List[Int], currentMax: Int = Int.MinValue): Int = {
//  if(list.isEmpty) currentMax
//  else if ( list.head > currentMax) max(list.tail, list.head)
//  else max(list.tail,currentMax)
//}



class HighestNumberOfCharactersReducer extends Reducer[Text,IntWritable,Text,IntWritable]{
  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
//    var sum = values.asScala.foldLeft(0)(_ + _.get)

//    def max(a: IntWritable, b: IntWritable): Int = {
//      if (a.get() > b.get()) a.get() else b.get()
//    }
    var m = values.asScala.foldLeft(0)(_ max _.get)


    context.write(key, new IntWritable(m))
  }
}
