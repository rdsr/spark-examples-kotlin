import org.apache.spark.api.java.JavaSparkContext

fun estimatePi(slices: Int) {
  val sc = JavaSparkContext("local", "SparkPi")
  val n = slices * 100000
  val count = sc
      .parallelize((1..n).toList())
      .map { ignore ->
        val x = Math.random()
        val y = Math.random()
        if (x*x + y*y < 1.0) 1 else 0 }
      .reduce { i, j -> i + j }
  println("Pi is roughly " + 4.0 * count / n)
}

fun main(args: Array<String>) {
  estimatePi(2)
}