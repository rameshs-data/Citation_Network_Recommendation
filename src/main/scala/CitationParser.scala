import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import net.liftweb.json.{DefaultFormats, _}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

object CitationParser{

//  define the journal schema
  case class Journal(
                      entities: List[String],
                      journalVolume: String,
                      journalPages: String,
                      pmid: String,
                      year: Option[Int],
                      outCitations: List[String],
                      s2Url: String,
                      s2PdfUrl: String,
                      id: String,
                      authors: List[Authors],
                      journalName: String,
                      paperAbstract: String,
                      inCitations: List[String],
                      pdfUrls: List[String],
                      title: String,
                      doi: String,
                      sources: List[String],
                      doiUrl: String,
                      venue: String)

//  define the author schema
  case class Authors(
                      name: String,
                      ids: List[String]
                    )

  def hex2dec(hex: String): BigInt = {
    hex.toLowerCase().toList.map(
      "0123456789abcdef".indexOf(_)).map(
      BigInt(_)).reduceLeft( _ * 16 + _)
  }

  def main(args: Array[String]): Unit = {

//    creating a spark context driver and setting log level to error
    val sc = new SparkContext("local[*]","Citation")
    sc.setLogLevel("ERROR")

//    reading the file using the spark context
    val lines = sc.textFile("file:///All Items Offline/Sem2/CS648 Project/sample_data/s2-corpus-00/s2-corpus-00")

//     println(s"Number of entries in linesRDD is ${lines.count()}") //1000000
//    extracting the data using lift json parser
    val journalRdd = lines.map(x => {implicit val formats = DefaultFormats;parse(x).extract[Journal]}).cache()

//    println(s"Number of entries in journalRDD is ${journalRdd.count()}") //1000000

//    printing the values of the journals
//    journalRdd.foreach(x => println(x.outCitations))

//     create journal RDD vertices with ID and Name
    val journalVertices = journalRdd.map(journal => (journal.id, journal.journalName)).distinct

//    println(s"Number of entries in journalVerticesRDD is ${journalVertices.count()}") //1000000

//    printing the values of the vertex
//    journalVertices.foreach(x => println(x._1))

//     Defining a default vertex called nocitation
    val nocitation = "nocitation"

//     Map journal ID to the journal name to be printed
//    val journalVertexMap = journalVertex.map(journal =>{ case ((journal._1), (journal._2)) => (journal._1 -> journal._2) })

//    Creating edges with outCitations and inCitations
    val citations = journalRdd.map(journal => (journal.id,journal.outCitations)).distinct
//    println(s"Number of entries in citationsRDD is ${citations.count()}") //1000000

//    printing citation values
//    citations.foreach(x => println(x._1,x._2))

//    creating citation edges with outCitations and inCitations
    val citationEdges = citations.map{
      case((id,outCitations)) => for(outCitation <- outCitations){
//        Edge(id.hex,outCitation.hex)
      }
    }
//    println(s"Number of entries in citationEdgesRDD is ${citationEdges.count()}")

    citationEdges.foreach(println)

//    val graph = Graph(citationEdges,journalVertices,nocitation)
  }
}