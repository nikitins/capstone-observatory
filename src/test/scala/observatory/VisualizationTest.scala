package observatory

import observatory.Visualization.predictTemperature
import org.scalatest.FunSuite
import org.scalatest.prop.Checkers
import Visualization._

trait VisualizationTest extends FunSuite with Checkers {
  test("test5") {
    val pairs = List((Location(1, 3), -3.0), (Location(5, 7), 10.0), (Location(5, 5), 15.0) )
    val l = Location(5, 5)
    assert(predictTemperature(pairs, l) === 15)
  }

  test("distance0") {
    val l = Location(10, 20)
    assert(0 === distance(l, l))
  }

  test("distancePI") {
    val l1 = Location(-8, -30)
    val l2 = Location(8, 150)
    assert(Math.PI * R === distance(l1, l2))
  }



  test("test7") {
    val list: Iterable[(Temperature, Color)] = List((-1.0,Color(255,0,0)), (1.148387E7,Color(0,0,255)))
    val value = 2870966.75
    val ans = Visualization.interpolateColor(list, value)
    assert(Color(191,0,64) === ans)
  }
//  test("test8") {
//    val list: Seq[(Location, Temperature)] = Seq((Location(90, -179), 10), (Location(80, -170), 20))
//    val location = Location(88.0,-176.0)
//    val ans = Visualization.predictTemperature(list, location)
//    assert(1 === ans)
//  }
  test("test9") {
    val list: Seq[(Location, Temperature)] = Seq((Location(90, 0), 10), (Location(80, 0), 20))
    val location = Location(83, 0)
    val ans = Visualization.predictTemperature(list, location)
    assert(ans >= 15)
    assert(ans <= 20)
  }
//
//  test("test99") {
//    val list: Seq[(Location, Temperature)] = Seq((Location(90, 0), 10), (Location(80, 0), 20))
//    val location = Location(90, -180)
//    val ans = Visualization.predictTemperature(list, location)
//    assert(ans >= 15)
//  }

//  test("test10") {
//    val list: Seq[(Location, Temperature)] = Seq((Location(90, 0), 10), (Location(80, 0), 20))
//    val location = Location(83, 0)
//    var x = 2.0
//    var mas: List[(Double, Double)] = List()
//    while(x < 100) {
//      Visualization.pp = x
//      val ans = Visualization.predictTemperature(list, location)
//      mas = mas ::: List((x, ans))
//      x += 1
//    }
//
//    assert(1 === mas)
//  }
//
//  test("test6") {
//    val temps = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
//    val colors: Iterable[(Temperature, Color)] = Seq(
//      (-60, Color(0, 0, 0)),
//      (-50, Color(33, 0, 107)),
//      (-27, Color(255, 0, 255)),
//      (-15, Color(0, 0, 255)),
//      (0, Color(0, 255, 255)),
//      (12, Color(255, 255, 0)),
//      (32, Color(255, 0, 0)),
//      (60, Color(255, 255, 255)))
//    val ever = Extraction.locationYearlyAverageRecords(temps)
//    val img = Visualization.visualize(ever, colors)
//    img.output(new java.io.File("target/some-image4.png"))
//  }
}
