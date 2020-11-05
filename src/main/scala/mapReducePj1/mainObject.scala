package mapReducePj1

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object mainObject extends App {
  if(args.length != 2) {
    println("Usage: mainObject <input dir> <output dir>")
    System.exit(-1)
  }
// starting the job object used for our mapreduce
  val job = Job.getInstance()

  job.setJarByClass(mainObject.getClass)

  job.setJobName("Map Reducer for Pj1")

  //job.setInputFormatClass(classOf[TextInputFormat]) //TODO: CHECK IF NEEDS TO BE CHANGED

  FileInputFormat.setInputPaths(job, new Path(args(0)))
  FileOutputFormat.setOutputPath(job, new Path(args(1)))


  //set mapper and reducer classes
  job.setMapperClass(classOf[pj1Mapper])
  job.setReducerClass(classOf[pj1Reducer])

  //specify job's output key and value classes
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])


  val success = job.waitForCompletion(true)
  System.exit(if(success) 0 else 1)
}
