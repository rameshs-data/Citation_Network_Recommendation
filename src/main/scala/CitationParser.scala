import org.apache.spark.{SparkConf, SparkContext}
import net.liftweb.json.{DefaultFormats, _}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import sun.security.provider.certpath.Vertex

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
    val journalRdd: RDD[Journal] = lines.map(x => {implicit val formats: DefaultFormats.type = DefaultFormats;parse(x).extract[Journal]}).cache()

//    println(s"Number of entries in journalRDD is ${journalRdd.count()}") //1000000

//    printing the values of the journals
//    journalRdd.foreach(x => println(x.outCitations))

//     create journal RDD vertices with ID and Name
    val journalVertices: RDD[(Long, String)] = journalRdd.map(journal => (hex2dec(journal.id).toLong, journal.journalName)).distinct

//    println(s"Number of entries in journalVerticesRDD is ${journalVertices.count()}") //1000000

//    printing the values of the vertex
//    journalVertices.foreach(x => println(x._1))

//     Defining a default vertex called nocitation
    val nocitation = "nocitation"

//     Map journal ID to the journal name to be printed
//    val journalVertexMap = journalVertices.map(journal =>{
//      case (journal._1, name) =>
//        journal._1 -> name
//    }).collect.toList.toMap

//    Creating edges with outCitations and inCitations
    val citations= journalRdd.map(journal => ((hex2dec(journal.id).toLong,journal.outCitations),1)).distinct
//    println(s"Number of entries in citationsRDD is ${citations.count()}") //1000000

//    printing citation values
//    citations.foreach(x => println(x._1,x._2))

//    creating citation edges with outCitations and inCitations
//    val citationEdges= citations.map{
//      case(id,outCitations) => for(outCitation <- outCitations){
//        val longOutCit = hex2dec(outCitation).toLong
////        println(id,longOutCit)
//        Edge(id,hex2dec(outCitation).toLong)
//      }
//    }

//        creating citation edges with outCitations and inCitations

    val citationEdges= citations.flatMap{
          case((id,outCitations),num) =>
            outCitations.map(outCitation => Edge(id,hex2dec(outCitation).toLong,num))
        }

//    val citationEdges= citations.map{ case(id,outCitations) => outCitations.foreach(outCitation => Edge(id,hex2dec(outCitation).toLong))}}

//    val citationEdges = citations.map {
//      case (id, outCitations) =>Edge(org_id.toLong, dest_id.toLong, distance) }

    //    println(s"Number of entries in citationEdgesRDD is ${citationEdges.count()}")

//    println(s"${citationEdges.take(10).foreach(println)}")
//    citationEdges.foreach(println)

    println("creating graph")
    val graph = Graph(journalVertices,citationEdges,nocitation)
    println("graph created")

//    println(s"Total Number of journals: ${graph.numVertices}")
//    println(s"Total Number of citations: ${graph.numEdges}")

//    println("printing vertices")
//    println(s"${graph.vertices.take(10).foreach(println)}")
//    println("printing edges")
//    println(s"${graph.edges.take(10).foreach(println)}")

//    println("filter edge")
//    graph.edges.filter { case ( Edge(org_id, dest_id,distance))=> distance > 1000}.take(3)

    // use pageRank
//    val ranks = graph.pageRank(0.1).vertices
//    // join the ranks  with the map of airport id to name
//    val temp= ranks.join(journalVertices)
//    temp.take(1)
//
//    // sort by ranking
//    val temp2 = temp.sortBy(_._2._1, false)
//    temp2.take(2)
//
//    // get just the airport names
//    val impAirports =temp2.map(_._2._2)
//    impAirports.take(4)
//    //res6: Array[String] = Array(ATL, ORD, DFW, DEN)

    println("Finding the most influential citations")

    val ranks = graph.pageRank(0.0001).vertices
    ranks
      .join(journalVertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .take(10) // get the top 10
      .foreach(x => println(x._2._2))

    println("end")
  }
}