package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import observatory.Visualization.{interpolateColorSorted, predictTemperature}

/**
  * 4th milestone: value-added information
  */
object Manipulation {
  val h = 180
  val w = 360

  def locationToField(l: GridLocation): Int = {
    w * ((h / 2) - l.lat) + ((w / 2) + l.lon)
  }

  def prepareArray: Array[Temperature] = {
    val arr: Array[Temperature] = new Array(h * w)
    for (i <- arr.indices) {
      arr(i) = Double.NaN
    }
    arr
  }

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val arr: Array[Temperature] = prepareArray

    def fun(l: GridLocation): Temperature = {
      val id = locationToField(l)
      if (arr(id).isNaN) {
        arr(id) = predictTemperature(temperatures, Location(l.lat, l.lon))
      }

      arr(id)
    }
    fun
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
//    val arr: Array[Temperature] = prepareArray
//
//    def fun(l: GridLocation): Temperature = {
//      val id = locationToField(l)
//
//      if (arr(id).isNaN) {
//        var sum = 0.0
//        for (temps <- temperaturess) {
//          sum += makeGrid(temps)(l)
//        }
//        arr(id) = sum / temperaturess.size
//      }
//      arr(id)
//    }
//    fun
    ???
  }

  /**
    * @param temperatures Known temperatures
    * @param normals A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
//    val arr: Array[Temperature] = prepareArray
//
//    def fun(l: GridLocation): Temperature = {
//      val id = locationToField(l)
//
//      if (arr(id).isNaN) {
//        arr(id) = makeGrid(temperatures)(l) - normals(l)
//      }
//      arr(id)
//    }
//    fun
    ???
  }


}

