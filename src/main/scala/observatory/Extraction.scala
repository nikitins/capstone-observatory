package observatory

import java.time.LocalDate

import Main._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * 1st milestone: data extraction
  */
object Extraction {
  def toDouble(s: String): Double = {
    val x = if(s.startsWith("+")) s.substring(1) else s
    x.toDouble
  }

  def toC(f: Double): Double = {

    (f.toDouble - 32.0) / 1.8
  }

  def time(t: Long, m: String = ""): Long = {
    println((System.currentTimeMillis() - t) + " " + m)
    System.currentTimeMillis()
  }

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
//    var t = System.currentTimeMillis()
    val stationLines = scala.io.Source.fromInputStream(getClass.getResourceAsStream(stationsFile)).getLines() //.par
//    t = time(t, "to arr")
    val stations = stationLines.map(_.split(","))
      .filter(x => x.length == 4 && !(x(2).isEmpty || x(3).isEmpty)).map(x => ((x(0), x(1)), (x(2), x(3)))).toMap
//    t = time(t)


    val tempLines = scala.io.Source.fromInputStream(getClass.getResourceAsStream(temperaturesFile)).getLines() //.par
//    t = time(t)
    val temps = tempLines.map(_.split(","))
      .filter(x => x.length == 5 && !(x(2).isEmpty || x(3).isEmpty || x(4).isEmpty))
      .map(x => ((x(0), x(1)), (x(2), x(3), x(4))))
//    t = time(t)
    val data = temps.filter(x => stations.contains(x._1)).map(x => (LocalDate.of(year, x._2._1.toInt, x._2._2.toInt),
      Location(toDouble(stations(x._1)._1), toDouble(stations(x._1)._2)), toC(x._2._3.toDouble)))
//    t = time(t, "fin")
    val ans = data.toIterable
//    t = time(t, "toList")
    ans
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
//    var t = System.currentTimeMillis()
//    println("start average")
    val ans = records.par.map(f => (f._2, (1, f._3))).groupBy(f => f._1)
      .map(f => (f._1, f._2.aggregate((0, 0.0))((x, y) => (x._1 + y._2._1, x._2 + y._2._2), (x, y) => (x._1 + y._1, x._2 + y._2))))
      .map(f => (f._1, f._2._2 / f._2._1)).toList
//    time(t, "average")

    ans
  }

}
