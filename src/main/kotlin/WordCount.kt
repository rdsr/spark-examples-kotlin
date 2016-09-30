import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.io.File

fun wordCount() {
  val inputFile = File("build.gradle")
  val outputFile = File("build/wordcount")

  val conf =  SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
  val sc = JavaSparkContext(conf)
  val textFile = sc.textFile("file://" + inputFile.absolutePath)
  textFile
      .flatMap { l -> l.split(" ").iterator() }
      .mapToPair { w -> Tuple2(w, 1) }
      .reduceByKey { x, y -> x + y }
      .saveAsTextFile("file://" + outputFile.absolutePath)
}

fun main(args: Array<String>) {
  wordCount()
}