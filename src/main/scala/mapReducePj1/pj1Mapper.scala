package mapReducePj1


import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

import scala.collection.mutable.ListBuffer

/** WordMapper defines the map task for a basic wordcount mapreduce
 *
 * To define a map function, we need to specify the input types for key value
 * pairs and output types for key value pairs.  This is accomplished with generics
 * We'll use hadoop io types: Text instead of String, IntWritable instead of Int...
 * */
class pj1Mapper extends Mapper[LongWritable, Text, Text, IntWritable] {

  /**
   * map takes a line number, text content key value pair and a context,
   * and writes output key value pairs consisting of word, 1 to the context
   *
   * @param key     line number
   * @param value   text content of line
   * @param context context that output is written to
   */

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context): Unit = {

    val line = value.toString()

    // split line on all non-word characters to get array of words
    // filter out 0 length words
    // convert them all to uppercase
    // write each in a key value pair to the context

    //for clickstream info
    var entry = line.split("\\s").filter(_.length > 0).map(_.toString).toList
    if (entry.length == 4) {
     //if (entry(2) == "link") { //by commenting out this line, the end bracket for it and changing Text(entry(0)) to Text(entry(1))
        //you go from how many times someone clicked on a link from a page to how many times someone viewed a page
        context.write(new Text(entry(0)), new IntWritable(entry(3).toInt))
         //}
      }




    /*var entry = line.split("\\s").filter(_.length > 0).map(_.toString).toList //foreach(
    if (entry.length == 4 && (entry.head == "en" || entry.head == "en.m"))
    //if (entry.head =="EN") //|| entry.head == "en.m"))
      context.write(new Text(entry(1)), new IntWritable(entry(2).toInt))
*/

    }
  }

