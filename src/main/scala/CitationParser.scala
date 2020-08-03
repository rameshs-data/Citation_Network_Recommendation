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

    val env = prop.getProperty("env")
    val spark = if (env.equals("local")) {
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .appName("Citation")
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError='kill -9 %p'")
        .appName("Citation")
        .getOrCreate()
    }
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //    Reading file to RDD
    println("Reading Input...")
    val lines_orig = sc.textFile(prop.getProperty("file.path")) //spark context code
    //    val lines_orig = spark.read.text(prop.getProperty("file.path"))

    println("Initial paritition size:" + lines_orig.partitions.size)
    val lines_sample = lines_orig.sample(false, prop.getProperty("sample.size").toDouble, 2)
    println("Input data loaded!")

    println("Sample paritition size:" + lines_sample.partitions.size)

    val repart_size = prop.getProperty("repartition.size").toInt
    val lines = lines_sample.repartition(repart_size)

    //    printing the number of records
    println(s"Number of entries in input data is ${lines.count()}")

    //    extracting the data using lift json parser
    //    println("Extracting the data using lift json parser...")
    val publicationsRdd: RDD[Publication] = lines.map(line => {
      implicit val formats: DefaultFormats.type = DefaultFormats;
      parse(line).extract[Publication]
    }).cache()
    //    println("publicationRdd created!")

    if (testMode.equals("true")) {
      val tstEntyPblctn = prop.getProperty("test.entity.publication")
      val tstEntyJrnl = prop.getProperty("test.entity.journal")
      val testPrintMode = prop.getProperty("test.print.results")
      val taskMetrics = ch.cern.sparkmeasure.TaskMetrics(spark)

      if (tstEntyPblctn.equals("true")) {

        Utils.prntHdngLne("TESTING PUBLICATIONS")
        //        this retrieves the task metrics of this code snippet
        Publication.prfmChkPblctns(prop, sc, publicationsRdd, testPrintMode, taskMetrics)
        Utils.prntGrphTstMtrcs(spark)
        Utils.prntHdngLne("TESTING PUBLICATIONS COMPLETED")
      }
      if (tstEntyJrnl.equals("true")) {
        Utils.prntHdngLne("TESTING JOURNALS")
        //        this retrieves the task metrics of this code snippet
        Journal.prfmChkJrnls(prop, sc, publicationsRdd, testPrintMode, taskMetrics)
        Utils.prntGrphTstMtrcs(spark)
        Utils.prntHdngLne("TESTING JOURNALS COMPLETED")
      }
    } else {
      Utils.prntHdngLne("CITATION NETWORK ANALYSIS USING GRAPHX AND APACHE SPARK")
      println("Please select one of the below options:")
      println("1 : To search for a Publication")
      println("2 : To search for a Journal")
      println("3 : To Exit!")
      print("Enter here:")

      Iterator.continually(scala.io.StdIn.readInt)
        .takeWhile(_ != 3)
        .foreach {
          input =>
            input match {
              case 1 => {
                //  Searching for publications
                val pblctnDgrGrph = Publication.getPublicationGraph(sc, publicationsRdd)

                println("Select the below option for old or new publications:")
                println("1 : To search for a New publications")
                println("2 : To search for an Old publications")
                println("3 : To search for both")
                println("Enter here:")

                val searchMode = scala.io.StdIn.readInt match {
                  case 1 => EdgeDirection.In
                  case 2 => EdgeDirection.Out
                  case 3 => EdgeDirection.Either
                }

                println("Please enter the publication name or X to exit:")
                Iterator.continually(scala.io.StdIn.readLine)
                  .takeWhile(_ != "X")
                  .foreach {
                    searchPublication =>
                      pblctnDgrGrph.collectNeighbors(searchMode).lookup((pblctnDgrGrph.vertices.filter {
                        journal => (journal._2.publicationName.equals(searchPublication))
                      }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr).foreach(publication => println("Publication connected to the search in order of importance:"+publication._2.publicationName+"\n:pagerank:"+publication._2.pr)))
                      println("Please enter the publication name or X to exit:")
                  }
                pblctnDgrGrph.unpersist()
              }
              case 2 => {
                //  Searching for journal
                val jrnlDgrGrph = Journal.getJournalGraph(sc, publicationsRdd)

                println("Select the below option for old or new journals:")
                println("1 : To search for new Journals")
                println("2 : To search for Old Journals")
                println("3 : To search for both")

                val searchMode = scala.io.StdIn.readInt match {
                  case 1 => EdgeDirection.In
                  case 2 => EdgeDirection.Out
                  case 3 => EdgeDirection.Either
                }

                println("Please enter the journal name or X to exit:")

                Iterator.continually(scala.io.StdIn.readLine)
                  .takeWhile(_ != "X")
                  .foreach {
                    searchJournal =>
                      jrnlDgrGrph.collectNeighbors(searchMode).lookup((jrnlDgrGrph.vertices.filter {
                        journal => (journal._2.journalName.equals(searchJournal))
                      }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println("Journal connected to the search in order of importance:"+journal._2.journalName+"\n:pagerank:"+journal._2.pr)))
                      println("Please enter the journal name or X to exit:")
                  }
                jrnlDgrGrph.unpersist()
              }
              case _ => {
                println("Invalid Input!")
              }
            }
            println("Please select one of the below options:")
            println("1 : To search for a Publication")
            println("2 : To search for a Journal")
            println("3 : To Exit!")
            print("Enter here:")
        }
    }

    //closing cached data
    publicationsRdd.unpersist()
    //    sc.stop()
    spark.close()
    Utils.prntHdngLne("CNA APP CLOSED")
  }
}