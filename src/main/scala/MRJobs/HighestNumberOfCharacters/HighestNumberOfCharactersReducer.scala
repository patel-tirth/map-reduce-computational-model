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


class HighestNumberOfCharactersReducer extends Reducer[Text,IntWritable,Text,IntWritable]{


  override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
    var sum = values.asScala.foldLeft(0)(_ max _.get)
    // find the maximum from the values, this is the max character count
    context.write(key, new IntWritable(sum))
  }
}
