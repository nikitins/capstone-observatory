package observatory

import java.io.File

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.concurrent.TrieMap
import Interaction._

trait InteractionTest extends FunSuite with Checkers {
//  test("tile1") {
//    val tile = Tile(0, 0, 0)
//    assert(Location(0, 0) === getDownRigth(tile, 1))
//  }
//
//  test("main") {
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
//    val img = tile(ever, colors, Tile(0, 0, 1))
//    img.output(new java.io.File("target/some-tile3.png"))
//  }

//  test("generate") {
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
//
//    val base = "target/temperatures/2015/"
//    var time = System.currentTimeMillis()
//
//    def output(name: String): Unit = {
//
//    }
//
//    def fun(t: Tile, zoom: Int): Unit = {
//      if(zoom > 0) {
//        val ti = (System.currentTimeMillis() - time) / 1000
//        time = System.currentTimeMillis()
//        println(t + " " + ti / 60 + ":" + ti % 60)
//
//        val x = t.x
//        val y = t.y
//        val z = t.zoom
//        val img = tile(ever, colors, t)
//        val pref = base + z
//        new File(pref).mkdirs()
//        img.output(new java.io.File(pref + "/" + x + "-" + y + ".png"))
//        fun(Tile(x + x, y + y, z + 1), zoom - 1)
//        fun(Tile(x + x, y + y + 1, z + 1), zoom - 1)
//        fun(Tile(x + x + 1, y + y, z + 1), zoom - 1)
//        fun(Tile(x + x + 1, y + y + 1, z + 1), zoom - 1)
//      }
//    }
//
//    fun(Tile(0, 0, 0), 4)
//  }

}
