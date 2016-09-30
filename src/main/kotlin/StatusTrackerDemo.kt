import org.apache.spark.api.java.JavaSparkContext

fun statusTrackerDemo() {
  val sc = JavaSparkContext("local[2]", "StatusTrackerDemo")
  val rdd = sc
      .parallelize(arrayListOf(1, 2, 3, 4, 5), 5)
      .map { x -> Thread.sleep(2 * 1000); x }

  val jobFuture = rdd.collectAsync()
  while (!jobFuture.isDone) {
    Thread.sleep(1000)
    val jobIds = jobFuture.jobIds()
    if (jobIds.isEmpty()) {
      continue
    }

    val curJobId = jobIds.get(0)
    val jobInfo = sc.statusTracker().getJobInfo(curJobId)
    val stageInfo = sc.statusTracker().getStageInfo(jobInfo.stageIds()[0])
    println("Total tasks: " + stageInfo.numTasks() +
        " Active: " + stageInfo.numActiveTasks() +
        " Completed:  " + stageInfo.numCompletedTasks())
  }
  println("Job results are: " + jobFuture.get())
}

fun main(args: Array<String>) {
  statusTrackerDemo()
}