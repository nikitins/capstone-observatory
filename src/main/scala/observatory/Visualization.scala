package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Math._

import scala.collection.GenSeqLike

/**
  * 2nd milestone: basic visualization
  */
object Visualization {
  var pp: Double = 6
  var R = 6371.0
  def equal(x: Double, y: Double): Boolean = abs(x - y) <= 1e-9

  def distance(l1: Location, l2: Location): Double = {
    def getSig: Double = {
      if (equal(l1.lat, l2.lat) && equal(l1.lon, l2.lon))
        0
      else if (equal(l1.lat, -l2.lat) && equal(abs(l1.lon - l2.lon), 180))
        PI
      else {
        val f1 = l1.lat * PI / 180
        val f2 = l2.lat * PI / 180
        val a1 = l1.lon * PI / 180
        val a2 = l2.lon * PI / 180
        val df = abs(f1 - f2)
        val da = abs(a1 - a2)
        acos(sin(f1) * sin(f2) + cos(f1) * cos(f2) * cos(da))
      }
    }
//    val sigma = 2 * asin(sqrt(pow(sin(df/2), 2) + cos(f1) * cos(f2) * pow(sin(da / 2) , 2)))
    val sigma = getSig
    val ans = sigma * R
    if(ans < 1.0) 0 else ans
  }

  def wi(x: Location, xi: Location): Double = 1.0 / pow(distance(x, xi), pp)
  def wi(d: Double): Double = 1.0 / pow(d, pp)

  /**
    * @param temperatures Known temperatures: pairs containing a location and the temperature at this location
    * @param location Location where to predict the temperature
    * @return The predicted temperature at `location`
    */
  def predictTemperature(temperatures: Iterable[(Location, Temperature)], location: Location): Temperature = {
    val parTemp = temperatures.map(x => (x._1, x._2, distance(x._1, location)))

    for ((_, t, d) <- parTemp) {
      if (d < 1.0)
        return t
    }

    var up = 0.0
    var down = 0.0

    for ((l, t, d) <- parTemp) {
      val w = wi(d)
      up += w * t
      down += w
    }

//    val (up, down) = parTemp.aggregate((0.0, 0.0))((s, temp) => {
////      if (temp._3 < 5000) {
////        val w = wi(location, temp._1, temp._3)
////        (s._1 + w * temp._2, s._2 + w)
////      } else
////        (s._1, s._2)
//      val w = wi(location, temp._1, temp._3)
//      (s._1 + w * temp._2, s._2 + w)
//    }, (s1, s2) => (s1._1 + s2._1, s1._2 + s2._2))
    up / down
  }

  /**
    * @param points Pairs containing a value and its associated color
    * @param value The value to interpolate
    * @return The color that corresponds to `value`, according to the color scale defined by `points`
    */
  def interpolateColor(points: Iterable[(Temperature, Color)], value: Temperature): Color = {
    val arr = points.toArray.sortBy(_._1)
    interpolateColorSorted(arr, value)
  }

  def interpolateColorSorted(points: Seq[(Temperature, Color)], value: Temperature): Color = {
    if (points.head._1 >= value)
      points.head._2
    else if (points.last._1 <= value)
      points.last._2
    else {
      def inter(x: Double, x0: Double, y0: Double, x1: Double, y1: Double) = (y0 * (x1 - value) + y1 * (value - x0) ) / (x1 - x0)
      val id = points.lastIndexWhere(p => p._1 < value)
      if (id  == -1) {
        throw new IllegalArgumentException(s"$points value $value")
      }
      val x0 = points(id)._1
      val y0 = points(id)._2
      val x1 = points(id + 1)._1
      val y1 = points(id + 1)._2

      val r = inter(value, x0, y0.red, x1, y1.red)
      val g = inter(value, x0, y0.green, x1, y1.green)
      val b = inter(value, x0, y0.blue, x1, y1.blue)

      val ans = Color(r.round.toInt, g.round.toInt, b.round.toInt)
//      assert(ans.red >= 0 && ans.red < 256, s"$ans.red")
//      assert(ans.green >= 0 && ans.green < 256, s"$ans.green")
//      assert(ans.blue >= 0 && ans.blue < 256, s"$ans.blue")
      ans
    }
  }

  def superVisualizeCustom(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)],
                           h: Int, w: Int, topLeft: Location, downRight: Location) = {

    val dlon = downRight.lon - topLeft.lon
    val dlat = topLeft.lat - downRight.lat
    val lonstep = dlon / w
    val latstep = dlat / h

    def locationToField(l: Location): Int = {
      val ans = w * ((topLeft.lat - l.lat) / latstep).toInt + ((l.lon - topLeft.lon) / lonstep).toInt
      if (ans >= 65536) {
        val x = in(l)

      }
      ans
    }

    def fieldToLocation(id: Int): Location = {
      val x = id / w
      val y = id % w
      Location(topLeft.lat - x * latstep, topLeft.lon + y * lonstep)
    }

    def in(l: Location): Boolean = {
      topLeft.lon <= l.lon && l.lon <= downRight.lon && downRight.lat <= l.lat && l.lat <= topLeft.lat
    }

    val arr: Array[Pixel] = new Array(h * w)
    val sortedColors = colors.toArray.sortBy(_._1)

    val localTemps = temperatures.filter(x => in(x._1))

    for ((l, t) <- localTemps) {
      val c = interpolateColorSorted(sortedColors, t)
      arr(locationToField(l)) = Pixel(c.red, c.green, c.blue, 127)
    }

    var ids = arr.indices.filter(_ != null).par

    ids.foreach(id => {
      val t = predictTemperature(temperatures, fieldToLocation(id))
      val c = interpolateColorSorted(sortedColors, t)
      arr(id) = Pixel(c.red, c.green, c.blue, 127)
    })
    Image(w, h, arr)
  }

  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @return A 360×180 image where each pixel shows the predicted temperature at its location
    */
  def visualize(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)]): Image = {
    val h = 180
    val w = 360
    val h2 = h/2
    val w2 = w/2

    def locationToField(l: Location): Int = {
      val lat = min(max(-h2 + 1, l.lat), h2)
      val lon = min(max(-w2, l.lon), w2 - 1)

      val ans = w * (h2 - lat).toInt + (-w2 + lon).toInt
      //TODO error
      val m = h * w
//      assert(ans < (h * w), s"$l $ans")
//      assert(ans >= 0, s"$l $ans")
      ans
    }

    def fieldToLocation(id: Int): Location = {
      val x = id / w
      val y = id % w
      Location(h / 2 - x, -(w / 2) + y)
    }


    val arr: Array[Pixel] = new Array(h * w)
    val sortedColors = colors.toArray.sortBy(_._1)

    for ((l, t) <- temperatures) {
      val c = interpolateColorSorted(sortedColors, t)
      arr(locationToField(l)) = Pixel(c.red, c.green, c.blue, 255)
    }

    var ids = arr.indices.filter(_ != null).par

    ids.foreach(id => {
      val t = predictTemperature(temperatures, fieldToLocation(id))
      val c = interpolateColorSorted(sortedColors, t)
      arr(id) = Pixel(c.red, c.green, c.blue, 255)
    })
    Image(w, h, arr)
  }


}

