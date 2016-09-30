import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import scala.Tuple2
import java.util.*


fun generateGraph(): List<Tuple2<Int, Int>> {
  val numEdges = 200
  val rand = Random(42)
  val edges: MutableSet<Tuple2<Int, Int>> = mutableSetOf()

  while (edges.size < numEdges) {
    val f = rand.nextInt()
    val t = rand.nextInt()
    if (f != t) edges += Tuple2(f, t)
  }
  return edges.toList()
}

fun transitiveClosure(slices: Int) {
  val conf = SparkConf()
      .setAppName("TC")
      .setMaster("local")
  val sc = JavaSparkContext(conf)
  var tc = sc.parallelizePairs(generateGraph(), slices).cache()

  // Linear transitive closure: each round grows paths by one edge,
  // by joining the graph's edges with the already-discovered paths.
  // e.g. join the path (y, z) from the TC with the edge (x, y) from
  // the graph to obtain the path (x, z).

  // Because join() joins on keys, the edges are stored in reversed order.
  val edges = tc.mapToPair { x -> Tuple2(x._2, x._1) }

  var oldCount = 0L
  var nextCount = tc.count()

  do {
    oldCount = nextCount
    tc.union(tc
        .join(edges)
        .mapToPair { x -> Tuple2(x._2._2, x._2._1) })
        .distinct()
        .cache()
    nextCount = tc.count()
  } while (nextCount != oldCount)

  println("TC has " + tc.count() + " edges.")
}

fun main(args: Array<String>) {
  transitiveClosure(2)
}