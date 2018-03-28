package observatory

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

trait ManipulationTest extends FunSuite with Checkers {
//  val temps = Extraction.locateTemperatures(2015, "/stations.csv", "/2015.csv")
//  val ever = Extraction.locationYearlyAverageRecords(temps)
//
//  test("makeGrid") {
//    val get = Manipulation.makeGrid(ever)
//    val location = GridLocation(90, -180)
//
//    var time = System.currentTimeMillis()
//    val x1 = get(location)
//    val t1 = System.currentTimeMillis() - time
//
//    time = System.currentTimeMillis()
//    val x2 = get(location)
//    val t2 = System.currentTimeMillis() - time
//    assert(x1 === x2)
////    println(t1 + " " + t2)
////    assert(t1 > t2)
//  }
//
//  test("normal") {
//    val normal = ever.map(f => (f._1, f._2 - 1.0))
//
//    val fun = Manipulation.deviation(ever, Manipulation.makeGrid(normal))
//
//    assert(Math.abs(1.0 - fun(GridLocation(5, 10))) <= 1e-5)
//  }

//  test("toField") {
//    assert(0 == Manipulation.locationToField(GridLocation(90, -180)))
//  }

}