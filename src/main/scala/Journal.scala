import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

//  define the journal schema
case class Journal(
                    journalName: String,
                    publications: List[Publication]
                  )

//  define the journal graph schema with degrees
case class JournalWithDegrees(
                               jid: VertexId,
                               journalName: String,
                               inDeg: Int,
                               outDeg: Int,
                               pr: Double
                             )

object Journal {

  def prfmChkJrnls(prop: Properties, sc: SparkContext, publicationsRdd: RDD[Publication], testPrintMode: String, taskMetrics: ch.cern.sparkmeasure.TaskMetrics): Unit = {

    val jrnlDgrGrph = getJournalGraph(sc, publicationsRdd)
    val tstJrnlSze = prop.getProperty("test.journal.size").toInt
    val journalSamples = jrnlDgrGrph.vertices.sortBy(_._2.pr, false).take(tstJrnlSze)

    //    println("printing the sample journal names:")
    //    journalSamples.foreach(println)

    println("Querying for top " + tstJrnlSze + " journals with highest pagerank:")
    taskMetrics.runAndMeasure(if (testPrintMode.equals("true")) {
      journalSamples.foreach {
        searchJournal =>
          val timedResult = Utils.time {
            println("Retrieving influential journals for: " + searchJournal._2.journalName)
            jrnlDgrGrph.collectNeighbors(EdgeDirection.Either).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr).foreach(journal => println(journal._2)))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    } else {
      journalSamples.foreach {
        searchJournal =>
          val timedResult = Utils.time {
            jrnlDgrGrph.collectNeighbors(EdgeDirection.Either).lookup((jrnlDgrGrph.vertices.filter {
              journal => (journal._2.journalName.equals(searchJournal._2.journalName))
            }.first)._1).map(journal => journal.sortWith(_._2.pr > _._2.pr))
          }
          //        println("Time taken :" +{timedResult.durationInNanoSeconds})
          println(timedResult.durationInNanoSeconds.toMillis + ",")
      }
    })
  }

  def getJournalGraph(sc: SparkContext, publicationsRdd: RDD[Publication]) = {

    //    create journal RDD vertices with publications
    val publicationsRdd_nonempty = publicationsRdd.filter(publication => publication.journalName != "")
    val publicationGroups = publicationsRdd_nonempty.groupBy(publication => publication.journalName)
    val journalRDD: RDD[Journal] = publicationGroups.map(publication => Journal(publication._1, publication._2.toList)).distinct

    println("creating journal vertices...")
    val journalWithIndex = journalRDD.zipWithIndex()
    val journalVertices = journalWithIndex.map { case (k, v) => (v, k) }

    val journalVertices2 = journalVertices.map {
      case (k, v) => (k, v.journalName)
    }

    val journalPublicDict = sc.broadcast(journalVertices.flatMap {
      case (jid, journal) =>
        journal.publications.map(p => (p.id, jid))
    }.collectAsMap())

    println("journal vertices created!")

    val nocitation = "nocitation"

    val jrnlEdgsWthDplcts1 = journalVertices.flatMap {
      case (jid, journal) =>
        journal.publications.flatMap(
          publication => publication.outCitations.map(
            outCitation => (jid,
              journalPublicDict.value.getOrElse(outCitation, -1.toLong)
              , 1)))
    }

    val jrnlEdgsWthDplcts2 = jrnlEdgsWthDplcts1.filter(dupEdgs1 => dupEdgs1._1 != dupEdgs1._2).filter(dupEdgs1 => dupEdgs1._2 != -1)

    val zeroVal = 0
    val addToCounts = (acc: Int, ele: Int) => (acc + ele)
    val sumPartitionCounts = (acc1: Int, acc2: Int) => (acc1 + acc2)

    println("creating journal edges...")
    val journalEdges = jrnlEdgsWthDplcts2.map(dupEdgs2 => ((dupEdgs2._1, dupEdgs2._2), dupEdgs2._3)).aggregateByKey(0)(addToCounts, sumPartitionCounts).map(unqEdgs => Edge(unqEdgs._1._1, unqEdgs._1._2, unqEdgs._2))
    println("journal edges created!")

    val jrnlGrphWthotDgrs = Graph(journalVertices2, journalEdges, nocitation)

    val inDegrees = jrnlGrphWthotDgrs.inDegrees
    val outDegrees = jrnlGrphWthotDgrs.outDegrees
    val pageRankTimed = Utils.time {
      jrnlGrphWthotDgrs.pageRank(0.0001).vertices
    }
    println("Time taken to generate journal pageranks:"+pageRankTimed.durationInNanoSeconds.toMillis)
    val pageRank = pageRankTimed.result

    println("creating journal graph with degrees...")
    //    creating journal graph with degrees
    val JournalGraph = jrnlGrphWthotDgrs.mapVertices {
      case (jid, jname) =>
        JournalWithDegrees(jid, jname, 0, 0, 0.0)
    }

    val jrnlDgrGrph: Graph[JournalWithDegrees, Int] = JournalGraph.outerJoinVertices(inDegrees) {
      (jid, j, inDegOpt) => JournalWithDegrees(jid, j.journalName, inDegOpt.getOrElse(0), j.outDeg, j.pr)
    }.outerJoinVertices(outDegrees) {
      (jid, j, outDegOpt) => JournalWithDegrees(jid, j.journalName, j.inDeg, outDegOpt.getOrElse(0), j.pr)
    }.outerJoinVertices(pageRank) {
      (jid, j, prOpt) => JournalWithDegrees(jid, j.journalName, j.inDeg, j.outDeg, prOpt.getOrElse(0))
    }

    prntJtnlGrphSmry(jrnlGrphWthotDgrs,inDegrees,outDegrees,pageRank)

    println("journal graph with degrees and page rank added")
    jrnlDgrGrph.cache()
  }

  //  method to print journal graph summary
  def prntJtnlGrphSmry(jrnlGrphWthotDgrs: Graph[String, Int], inDegrees: VertexRDD[Int], outDegrees: VertexRDD[Int], pageRank: VertexRDD[Double]): Unit = {

    val vrtcsCnt = jrnlGrphWthotDgrs.vertices.count
    val edgsCnt = jrnlGrphWthotDgrs.edges.count
    //    val inDegrees = pblctnGrphWthotDgr.inDegrees
    //    val outDegrees = pblctnGrphWthotDgr.outDegrees
    //    val maxInDegree = inDegrees.reduce((a, b) => (a._1, a._2 max b._2))
    //    val maxOutDegree = outDegrees.reduce(Utils.max)
    //    val maxDegrees = pblctnDgrGrph.degrees.reduce(Utils.max)
    //    val pageRank = pblctnGrphWthotDgr.pageRank(0.0001).vertices.distinct
    val pageRankList = pageRank.map(_._2).distinct

    Utils.prntSbHdngLne("Printing Journal Graph Summary")
    println("No. of journals:" + vrtcsCnt)
    println("No. of citations:" + edgsCnt)
    println("No. of in degrees:" + inDegrees.count)
    println("No. of out degrees:" + outDegrees.count)
    //    println("Highest in degree vertex:" + maxInDegree)
    //    println("Highest out degree vertex:" + maxOutDegree)
    //    println("Highest degree vertex:" + maxDegrees)
    println("Total unique page rank values found:" + pageRankList.count)
    println("Maximum Page rank:" + pageRankList.max)
    println("Minimum Page rank:" + pageRankList.min)
    println("Printing page rank values:")
    pageRankList.foreach(println)
    Utils.prntSbHdngEndLne()
  }
}
