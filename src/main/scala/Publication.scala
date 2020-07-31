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

  //define a method for performance check of publications
  def prfmChkPblctns(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String, taskMetrics: ch.cern.sparkmeasure.TaskMetrics): Unit = {
    val pblctnDgrGrph = getPublicationGraph(sc, publicationsRdd)
    val tstPblctnSze = prop.getProperty("test.publication.size").toInt
    val pblctnSmpls = pblctnDgrGrph.vertices.sortBy(_._2.pr, false).take(tstPblctnSze)

    //    println("printing the sample publication names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstPblctnSze + " publications with highest pagerank:")
    taskMetrics.runAndMeasure(
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
    )
  }

  //  define the method to get publication graph
  def getPublicationGraph(sc: SparkContext, publicationsRdd: RDD[Publication]): Graph[PublicationWithDegrees, Int] = {

    val pblctnsWthIndxRdd = publicationsRdd.zipWithIndex().map { case (k, v) => (v, k) }

    val pblctnIdDict = sc.broadcast(pblctnsWthIndxRdd.map(p => (p._2.id, p._1)).collectAsMap())

    //    create publication RDD vertices with ID and Name
    println("creating publication vertices...")
    val pblctnVrtcsWthInvldEntrs: RDD[(Long, String)] = publicationsRdd.map(publication => (pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.title)).distinct
    val publicationVertices = pblctnVrtcsWthInvldEntrs.filter(_._1 != 1)
    println("publication vertices created!")

    //     Defining a default vertex called nocitation
    val nocitation = "nocitation"

    println("creating citations...")
    val ctnEdgsWthInvldEntrs = publicationsRdd.map(publication => ((pblctnIdDict.value.getOrElse(publication.id, -1.toLong), publication.outCitations), 1)).distinct
    val citations = ctnEdgsWthInvldEntrs.filter(_._1 != 1)
    println("citations created!")

    println("creating citation edges with outCitations and inCitations...")
    //    creating citation edges with outCitations and inCitations
    val ctnEdgsExpndWthInvldEntrs = citations.flatMap {
      case ((id, outCitations), num) =>
        outCitations.map(outCitation => Edge(id, pblctnIdDict.value.getOrElse(outCitation, -1.toLong), num))
    }
    val citationEdges = ctnEdgsExpndWthInvldEntrs.filter(edge => edge.dstId != -1)
    println("citation edges created!")

    val pblctnGrphWthotDgr = Graph(publicationVertices, citationEdges, nocitation)

    println("creating publication graph with degrees...")
    //    creating publication graph with degrees
    val publicationGraph = pblctnGrphWthotDgr.mapVertices {
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

    //    get publication graph summary
    prntPblctnGrphSmry(pblctnGrphWthotDgr)

    println("publication graph with degrees and page rank added")
    pblctnDgrGrph.cache()
  }

  //  method to print publication graph summary
  def prntPblctnGrphSmry(pblctnGrphWthotDgr: Graph[String, Int]): Unit = {

    val vrtcsCnt = pblctnGrphWthotDgr.vertices.count
    val edgsCnt = pblctnGrphWthotDgr.edges.count
    val inDegrees = pblctnGrphWthotDgr.inDegrees
    val outDegrees = pblctnGrphWthotDgr.outDegrees
    val maxInDegree = inDegrees.reduce((a,b)=> (a._1,a._2 max b._2))
    val maxOutDegree = outDegrees.reduce(Utils.max)
    val maxDegrees = pblctnGrphWthotDgr.degrees.reduce(Utils.max)

    val pageRank = pblctnGrphWthotDgr.pageRank(0.0001).vertices.distinct
    val pageRankList = pageRank.map(_._2).distinct

    Utils.prntSbHdngLne("Printing Publication Graph Summary")
    println("No. of publications:" + vrtcsCnt)
    println("No. of citations:" + edgsCnt)

    println("No. of in degrees:" + inDegrees.count)
    println("No. of out degrees:" + outDegrees.count)

    println("Highest in degree vertex:" + maxInDegree)
    println("Highest out degree vertex:" + maxOutDegree)
    println("Highest degree vertex:" + maxDegrees)

    println("Total unique page rank values found:" + pageRankList.count)
    println("Maximum Page rank:" + pageRankList.max)
    println("Minimum Page rank:" + pageRankList.min)

    println("Printing page rank values:")
    pageRankList.foreach(println)
    Utils.prntSbHdngEndLne()
  }
}
