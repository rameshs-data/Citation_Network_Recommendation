import org.apache.spark.{SparkConf, SparkContext}

object citation {
  def main(args: Array[String]): Unit = {

    //creating a case Class for Journal
//    case class Journal(entities: Array[String], journalVolume: Int, journalPages: String, pmid: String, fieldsOfStudy: Array[String], year: Int, outCitations: Array[String], s2Url: String, s2PdfUrl: String, id: String, authors: Array[String], journalName: String, paperAbstract: String, inCitations: Array[String], pdfUrls: Array[String], title: String, doi: Array[String], sources: Array[String], doiUrl: Array[String], venue: String)

    val conf = new SparkConf().setAppName("citation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val textRDD = sc.textFile("file:///All Items Offline/Sem2/CS648 Project/sample_data/s2-corpus-00/s2-corpus-00")
    println("one")
    textRDD.take(10).foreach(println)
    println("two")
    sc.stop()
  }
}
