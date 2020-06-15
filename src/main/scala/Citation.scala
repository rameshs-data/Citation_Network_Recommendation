import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Failure

object Citation {
  def main(args: Array[String]): Unit = {

//    creating a case Class for Journal
//    case class Journal(entities: Array[String], journalVolume: Int, journalPages: String, pmid: String, fieldsOfStudy: Array[String], year: Int, outCitations: Array[String], s2Url: String, s2PdfUrl: String, id: String, authors: Array[String], journalName: String, paperAbstract: String, inCitations: Array[String], pdfUrls: Array[String], title: String, doi: Array[String], sources: Array[String], doiUrl: Array[String], venue: String)
    case class Journal(entities: String, journalVolume: String, journalPages: String, pmid: String, fieldsOfStudy: String, year: String, outCitations: String, s2Url: String, s2PdfUrl: String, id: String, authors: String, journalName: String, paperAbstract: String, inCitations: String, pdfUrls: String, title: String, doi: String, sources: String, doiUrl: String, venue: String){
}

//    function to parse input into Flight class
    def parseJournal(str: String): Journal = {
      val line = str.replaceAll("[\\{]|[\\}]","").split(",(?![^\\[]*[\\]])(?=(?:([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$))")
      Journal(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7), line(8), line(9), line(10), line(11), line(12), line(13), line(14), line(15), line(16), line(17),line(18),line(19))
    }

    val conf = new SparkConf().setAppName("citation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val textRDD = sc.textFile("file:///All Items Offline/Sem2/CS648 Project/sample_data/s2-corpus-00/s2-corpus-00")

    val journalsRDD = textRDD.map(x => parseJournal(x))


   journalsRDD.take(1)

//    println("Printing file content: START")
//    textRDD.take(10).foreach(println)
//    println("Printing file content: END")
    sc.stop()
  }
}
