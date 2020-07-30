import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

//  define the publication schema
case class Publication(
                        entities: List[String],
                        journalVolume: Option[String],
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

//  define the publication graph schema with degrees
case class PublicationWithDegrees(
                                   id: VertexId,
                                   publicationName: String,
                                   inDeg: Int,
                                   outDeg: Int,
                                   pr: Double
                                 )

object Publication {
  //  define the method to get publication graph
  def getPublicationGraph(sc: SparkContext, publicationsRdd: RDD[Publication]): Graph[PublicationWithDegrees, Int] = {

    val pblctnsWthIndxRdd = publicationsRdd.zipWithIndex().map { case (k, v) => (v, k) }

    val pblctnIdDict = sc.broadcast(pblctnsWthIndxRdd.map(p => (p._2.id, p._1)).collectAsMap())

    //    create publication RDD vertices with ID and Name
    println("creating publication vertices...")
    val publicationVertices: RDD[(Long, String)] = publicationsRdd.map(publication => (pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.title)).distinct
    println("publication vertices created!")

    //     Defining a default vertex called nocitation
    val nocitation = "nocitation"

    println("creating citations...")
    val citations = publicationsRdd.map(publication => ((pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.outCitations), 1)).distinct
    println("citations created!")

    println("creating citation edges with outCitations and inCitations...")
    //    creating citation edges with outCitations and inCitations
    val citationEdges = citations.flatMap {
      case ((id, outCitations), num) =>
        outCitations.map(outCitation => Edge(id, pblctnIdDict.value.getOrElse(outCitation, -1.toLong), num))
    }
    println("citation edges created!")

    println("creating publication graph with degrees...")
    //    creating publication graph with degrees
    val publicationGraph = Graph(publicationVertices, citationEdges, nocitation).mapVertices {
      case (pid, pname) =>
        PublicationWithDegrees(pid, pname, 0, 0, 0.0)
    }

    val inDegrees = publicationGraph.inDegrees
    val outDegrees = publicationGraph.outDegrees
    val pageRank = publicationGraph.pageRank(0.0001).vertices

    val pblctnDgrGrph: Graph[PublicationWithDegrees, Int] = publicationGraph.outerJoinVertices(inDegrees) {
      (pid, p, inDegOpt) => PublicationWithDegrees(p.id, p.publicationName, inDegOpt.getOrElse(0), p.outDeg, p.pr)
    }.outerJoinVertices(outDegrees) {
      (pid, p, outDegOpt) => PublicationWithDegrees(p.id, p.publicationName, p.inDeg, outDegOpt.getOrElse(0), p.pr)
    }.outerJoinVertices(pageRank) {
      (pid, p, prOpt) => PublicationWithDegrees(p.id, p.publicationName, p.inDeg, p.outDeg, prOpt.getOrElse(0))
    }

    println("publication graph with degrees and page rank added")
    pblctnDgrGrph.cache()
  }

  def prfmChkPblctns(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String): Unit = {
    val pblctnDgrGrph = getPublicationGraph(sc, publicationsRdd)
    val tstPblctnSze = prop.getProperty("test.publication.size").toInt
    val pblctnSmpls = pblctnDgrGrph.vertices.sortBy(_._2.pr, false).take(tstPblctnSze).drop(1)

    //    println("printing the sample publication names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstPblctnSze + " publications with highest pagerank:")

    if (testPrintMode.equals("true")) {
      pblctnSmpls.foreach {
        srchPblctn =>
          val timedResult = Utils.time {
            println("Retrieving influential publications for: " + srchPblctn._2.publicationName)
            pblctnDgrGrph.collectNeighbors(EdgeDirection.In).lookup((pblctnDgrGrph.vertices.filter {
              publication => (publication._2.publicationName.equals(srchPblctn._2.publicationName))
            }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr).foreach(publication => println(publication._2)))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    } else {
      pblctnSmpls.foreach {
        srchPblctn =>
          val timedResult = Utils.time {
            //            println("Retrieving influential journals for: " + srchPblctn._2.publicationName)
            pblctnDgrGrph.collectNeighbors(EdgeDirection.In).lookup((pblctnDgrGrph.vertices.filter {
              publication => (publication._2.publicationName.equals(srchPblctn._2.publicationName))
            }.first)._1).map(publication => publication.sortWith(_._2.pr > _._2.pr))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    }
  }
}
