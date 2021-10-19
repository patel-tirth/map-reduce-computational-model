import HelperUtils.CreateLogger
import HelperUtils.Parameters.logger
import MRJobs.MatchedPatterns.{PatternMessageDistMapper, PatternMessageDistReducer}
import MRJobs.TypesOfLogMessages.{TypesOfLogMessagesMapper, TypesOfLogMessagesReducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.{Logger, LoggerFactory}

object RunJobs {

  def main(args: Array[String]): Unit = {
//    val logger = CreateLogger(classOf[GenerateLogData.type])
//    val logger: Logger = LoggerFactory.getLogger(this.getClass)
//    logger.info("generated log files, now running map reduce")
//        logger.info("starting job 1 ")
//        val configuration = new Configuration
//        val job = Job.getInstance(configuration, "pattern match")
//        job.setJarByClass(this.getClass)
//        job.setMapperClass(classOf[PatternMessageDistMapper])
//        job.setCombinerClass(classOf[PatternMessageDistReducer])
//        job.setReducerClass(classOf[PatternMessageDistReducer])
//        job.setOutputKeyClass(classOf[Text])
//        job.setOutputKeyClass(classOf[Text]);
//        job.setOutputValueClass(classOf[IntWritable]);
//        FileInputFormat.addInputPath(job, new Path(args(0)))
//        FileOutputFormat.setOutputPath(job, new Path(args(1)))
//        job.waitForCompletion(true)

        val conf2 = new Configuration()
        val job2 = Job.getInstance(conf2, "pattern match")
        job2.setJarByClass(this.getClass)
        job2.setMapperClass(classOf[TypesOfLogMessagesMapper])
        job2.setCombinerClass(classOf[TypesOfLogMessagesReducer])
        job2.setReducerClass(classOf[TypesOfLogMessagesReducer])
        job2.setOutputKeyClass(classOf[Text])
        job2.setOutputKeyClass(classOf[Text]);
        job2.setOutputValueClass(classOf[IntWritable]);
        conf2.set("mapred.textoutputformat.separator", ",")
        FileInputFormat.addInputPath(job2, new Path(args(0)))
        FileOutputFormat.setOutputPath(job2, new Path(args(1) + "/job2"))

    System.exit(if(job2.waitForCompletion(true))  0 else 1)
  }
}
