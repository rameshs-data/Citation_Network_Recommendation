import net.liftweb.json.{DefaultFormats, _}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CitationParser {

  def main(args: Array[String]): Unit = {

    //    Getting the properties for the environment
    val prop = Utils.getProperties()
    //    checking for test flags
    val testMode = prop.getProperty("test.mode")

    //    Creating a spark configuration
    //    val conf = new SparkConf()
    //    conf.setAppName("Citation")
    //    Creating a spark context driver and setting log level to error
    //    val sc = new SparkContext(spark)//spark context code

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Citation")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //    Reading file to RDD
    println("Reading file to RDD...")
    val lines_orig = sc.textFile(prop.getProperty("file.path")) //spark context code
    //    val lines_orig = spark.read.text(prop.getProperty("file.path"))
    val lines = lines_orig.sample(false, prop.getProperty("sample.size").toDouble, 2)
    println("RDD created!")

    //    printing the number of records
    println(s"Number of entries in linesRDD is ${lines.count()}")

    //    extracting the data using lift json parser
    println("Extracting the data using lift json parser...")
    val publicationsRdd: RDD[Publication] = lines.map(line => {
      implicit val formats: DefaultFormats.type = DefaultFormats;
      parse(line).extract[Publication]
    }).cache()
    println("publicationRdd created!")

    if (testMode.equals("true")) {
      val testEntity = prop.getProperty("test.entity")
      val testPrintMode = prop.getProperty("test.print.results")
      val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)

      if (testEntity.equals("publication")) {
        //        this retrieves the task metrics of this code snippet
        taskMetrics.runAndMeasure(Publication.prfmChkPblctns(prop, sc, publicationsRdd, testPrintMode))
        spark.sql("show tables").show
        spark.sql("select * from PerfTaskMetrics").show
        val perfTaskMetrics = spark.sql("select stageId, count(*) as count, sum(executorRunTime) as sum_exctrRnTme, sum(executorCpuTime) as sum_exctrCpuTme " +
          "from PerfTaskMetrics " +
          "group by stageId " +
          "order by sum_exctrRnTme desc, sum_exctrCpuTme desc limit 1000")
        perfTaskMetrics.show
        //        perfTaskMetrics.repartition(1).write.format("com.databricks.spark.csv").save(prop.getProperty("test.task.metrics")) //commenting code to save the results to file
      }
      if (testEntity.equals("journal")) {
        //        this retrieves the task metrics of this code snippet
        taskMetrics.runAndMeasure(Journal.prfmChkJrnls(prop, sc, publicationsRdd, testPrintMode))
        spark.sql("show tables").show
        spark.sql("select * from PerfTaskMetrics").show
        val perfTaskMetrics = spark.sql("select stageId, count(*) as count, sum(executorRunTime) as sum_exctrRnTme, sum(executorCpuTime) as sum_exctrCpuTme " +
          "from PerfTaskMetrics " +
          "group by stageId " +
          "order by sum_exctrRnTme desc, sum_exctrCpuTme desc limit 1000")
        perfTaskMetrics.show
        //        perfTaskMetrics.repartition(1).write.format("com.databricks.spark.csv").save(prop.getProperty("test.task.metrics")) //commenting code to save the results to file
      }
    } else {
      println("Please select one of the below options:")
      println("1 : Search for a Publication")
      println("2 : Search for a Journal")
      print("Enter here:")

      val input = scala.io.StdIn.readInt

      input match {
        case 1 => {
          //  Searching for publications
          val publicationGraph = Publication.getPublicationGraph(sc, publicationsRdd)

          println("Please enter the publication name or X to exit:")

          Iterator.continually(scala.io.StdIn.readLine)
            .takeWhile(_ != "X")
            .foreach {
              searchPublication =>
                publicationGraph.collectNeighbors(EdgeDirection.In).lookup((publicationGraph.vertices.filter {
                  journal => (journal._2.publicationName.equals(searchPublication))
                }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr).foreach(publication => println(publication._2)))
                println("Please enter the publication name or X to exit:")
            }
        }
        case 2 => {
          //  Searching for journal
          val jrnlDgrGrph = Journal.getJournalGraph(sc, publicationsRdd)

          println("Please enter the journal name or X to exit:")

          Iterator.continually(scala.io.StdIn.readLine)
            .takeWhile(_ != "X")
            .foreach {
              searchJournal =>
                jrnlDgrGrph.collectNeighbors(EdgeDirection.In).lookup((jrnlDgrGrph.vertices.filter {
                  journal => (journal._2.journalName.equals(searchJournal))
                }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println(journal._2)))
                println("Please enter the journal name or X to exit:")
            }
        }
        case _ => println("Invalid Input! Exiting Now...!")
      }
    }

    //    drawGraph(degreeGraph)
    publicationsRdd.unpersist()
    sc.stop()
    println("end")
  }
}