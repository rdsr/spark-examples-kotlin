import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2

fun computePageRank(iters: Int) {
  val sc = JavaSparkContext("local", "PageRank")
  val lines = sc.textFile("data/pagerank_data.txt")

  val links = lines
      .mapToPair { l -> val split = l.split(Regex(" ")); Tuple2(split[0], split[1]) }
      .distinct()
      .groupByKey()
      .cache()
  var ranks = links.mapValues { 1.0 }

  for (i in 1..iters) {
    val contribs = links
        .join(ranks)
        .values()
        .flatMapToPair { t ->
          val urls = t._1
          val rank = t._2
          val size = urls.count()
          urls.map { url -> Tuple2(url, rank/size) }.iterator()
        }
    ranks = contribs
        .reduceByKey { x, y -> x + y }
        .mapValues { x -> 0.15 + 0.85 * x }
  }
  val output = ranks.collect()
  output.forEach { tup ->  println(tup._1 + " has rank: " + tup._2 + ".") }
}

fun main(args: Array<String>) {
  computePageRank(100)
}