package observatory

import java.time.LocalDate

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {
  test("test1") {
    assert(1 === 1)
  }

  test("test2") {
    val x = Extraction.locateTemperatures(2015, "/test_stations.csv", "/test_2015.csv")
    assert(x.isEmpty === false)
    val ans = Seq(
      (LocalDate.of(2015, 8, 11), Location(37.35, -78.433), 27.3),
      (LocalDate.of(2015, 12, 6), Location(37.358, -78.438), 0.0),
      (LocalDate.of(2015, 1, 29), Location(37.358, -78.438), 2.000000000000001)
    )
    assert(ans === x)
  }

  test("test3") {
    val x = Extraction.locateTemperatures(2015, "/test_stations.csv", "/test_2015.csv")
    val y = Extraction.locationYearlyAverageRecords(x)
    val expected = Seq(
      Location(37.35, -78.433) -> 27.3,
      Location(37.358, -78.438) -> 1.0000000000000004
    )
    assert(expected === y)
  }
}