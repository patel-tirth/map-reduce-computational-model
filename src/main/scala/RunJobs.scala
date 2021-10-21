import HelperUtils.CreateLogger
import HelperUtils.Parameters.logger
import MRJobs.HighestNumberOfCharacters.{HighestNumberOfCharactersMapper, HighestNumberOfCharactersReducer}
import MRJobs.MatchedPatterns.{PatternMessageDistMapper, PatternMessageDistReducer}
import MRJobs.SortedTimeIntervals.{CustomSort, SortedTimeIntervalMatchedMapper, SortedTimeIntervalMatchedReducer}
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
    val logger = LoggerFactory.getLogger(this.getClass)
      // input of type:
      // hadoop jar jar-file input-dir output-dir jobx
      // where  1<= x <= 4 representing job number
      if(args.size > 2){
          val jobs = args.toList.drop(2).distinct // only select jobs with different number
          for(job <- jobs){
              if(job=="job1"){
                  logger.info("starting job 1 ")
                  val configuration = new Configuration()
                  val job = Job.getInstance(configuration, "types of log message predefiend time interval ")
                  job.setJarByClass(this.getClass)
                  job.setMapperClass(classOf[TypesOfLogMessagesMapper])
                  job.setCombinerClass(classOf[TypesOfLogMessagesReducer])
                  job.setReducerClass(classOf[TypesOfLogMessagesReducer])
                  job.setOutputKeyClass(classOf[Text])
                  job.setOutputKeyClass(classOf[Text]);
                  job.setOutputValueClass(classOf[IntWritable]);
                  configuration.set("mapred.output.textoutputformat.separator", ",")
                  FileInputFormat.addInputPath(job, new Path(args(0)))
                  FileOutputFormat.setOutputPath(job, new Path(args(1) + "/job1"))
                  job.waitForCompletion(true)
              } else if (job=="job2"){
                    logger.info("starting job 2")
                    val conf2 = new Configuration()
                    val job2 = Job.getInstance(conf2, "sorted in descending")
                    job2.setJarByClass(this.getClass)
                    job2.setMapperClass(classOf[SortedTimeIntervalMatchedMapper])
                    job2.setMapOutputKeyClass(classOf[IntWritable])
                    job2.setMapOutputValueClass(classOf[Text])

                    job2.setReducerClass(classOf[SortedTimeIntervalMatchedReducer])
                    job2.setOutputKeyClass(classOf[Text])
                    job2.setOutputValueClass(classOf[IntWritable]);
                    conf2.set("mapred.output.textoutputformat.separator", ",")
                    job2.setSortComparatorClass(classOf[CustomSort])
                    FileInputFormat.addInputPath(job2, new Path(args(0)))
                    FileOutputFormat.setOutputPath(job2, new Path(args(1) + "/job2"))
                    job2.waitForCompletion(true)

              } else if (job=="job3"){
                    logger.info("starting job 3")
                    val conf3 = new Configuration()
                    val job3 = Job.getInstance(conf3, "types of log messages , info, warn, debug, error")
                    job3.setJarByClass(this.getClass)
                    job3.setMapperClass(classOf[PatternMessageDistMapper])
                    job3.setCombinerClass(classOf[PatternMessageDistReducer])
                    job3.setReducerClass(classOf[PatternMessageDistReducer])
                    job3.setOutputKeyClass(classOf[Text])
                    job3.setOutputKeyClass(classOf[Text]);
                    job3.setOutputValueClass(classOf[IntWritable]);
                    conf3.set("mapred.textoutputformat.separator", ",")

                    FileInputFormat.addInputPath(job3, new Path(args(0)))
                    FileOutputFormat.setOutputPath(job3, new Path(args(1) + "/job3"))
                    job3.waitForCompletion(true)
              } else if (job=="job4"){
                    logger.info("starting job 4 ")
                    val conf4 = new Configuration()
                    val job4 = Job.getInstance(conf4, "highest number of characters")
                    job4.setJarByClass(this.getClass)
                    job4.setMapperClass(classOf[HighestNumberOfCharactersMapper])
                    job4.setCombinerClass(classOf[HighestNumberOfCharactersReducer])
                    job4.setReducerClass(classOf[HighestNumberOfCharactersReducer])
                    job4.setOutputKeyClass(classOf[Text])
                    job4.setOutputValueClass(classOf[IntWritable]);

                    conf4.set("mapred.textoutputformat.separator", ",")
                    FileInputFormat.addInputPath(job4, new Path(args(0)))
                    FileOutputFormat.setOutputPath(job4, new Path(args(1) + "/job4"))
                    job4.waitForCompletion(true)
              }
          }
      }
//    System.exit(if(job3.waitForCompletion(true))  0 else 1)
  }
}
