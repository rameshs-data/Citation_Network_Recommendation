package jsontests

import java.io._
import scala.collection.mutable._
import java.util.Properties
import net.liftweb.json.DefaultFormats
import net.liftweb.json._

object JsonTest extends App {

  val stocksJsonString = """
{
  "phrases" : [
      "check stock prices",
      "get stock prices",
      "get stocks"
  ],
  "stocks": [
  { "stock": {
    "symbol": "AAPL",
    "name": "Apple",
    "price": "0"
  }},
  { "stock": {
    "symbol": "GOOG",
    "name": "Google",
    "price": "0"
  }}
  ]
}
"""

  implicit val formats = DefaultFormats  // for json handling
  case class Stock(val symbol: String, val name: String, var price: String)

  def getStocks(stocksJsonString: String): Array[Stock] = {
    val stocks = ArrayBuffer[Stock]()
    val json = JsonParser.parse(stocksJsonString)

    // (1) used 'values' here because 'extract' was not working in Eclipse
    val phrases = (json \\ "phrases").children  // List[JString]
    println("\nphrases: " + phrases)
    for (p <- phrases) {                        // for each JString
      println("p.values = " + p.values)
    }

    // (2) this works okay with "sbt run"
    println("\nphrases: " + phrases)
    for (p <- phrases) {
      val x = p.extract[String]
      println("x = " + x)
    }

    // (3) access the stocks
    val elements = (json \\ "stock").children
    for ( acct <- elements ) {
      val stock = acct.extract[Stock]
      stocks += stock
    }
    stocks.toArray
  }

  getStocks(stocksJsonString).foreach(println)

}