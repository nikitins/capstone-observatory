package observatory

import com.sksamuel.scrimage.{Image, Pixel}
import Math._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    Location(
      toDegrees(atan(sinh(PI * (1.0 - 2.0 * tile.y.toDouble / (1<<tile.zoom))))),
      tile.x.toDouble / (1<<tile.zoom) * 360.0 - 180.0)
  }

  def getDownRigth(tile: Tile, z: Int): Location = {
    if(z <= 0)
      tileLocation(tile)
    else
      getDownRigth(Tile(tile.x + tile.x + 1, tile.y + tile.y + 1, tile.zoom + 1), z - 1)
  }



  /**
    * @param temperatures Known temperatures
    * @param colors Color scale
    * @param tile Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val topLeft = tileLocation(tile)
    val downRight = getDownRigth(tile, 8)
//    def in(l: Location) = topLeft.lon <= l.lon && l.lon <= downRight.lon && downRight.lat <= l.lat && l.lat <= topLeft.lat

//    val parTemps = temperatures.filter(x => in(x._1))

    Visualization.superVisualizeCustom(temperatures, colors, 256, 256, topLeft, downRight)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    * @param yearlyData Sequence of (year, data), where `data` is some data associated with
    *                   `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
    yearlyData: Iterable[(Year, Data)],
    generateImage: (Year, Tile, Data) => Unit
  ): Unit = {
    def allTiles(tile: Tile, zoom: Int): List[Tile] = {
      if (zoom == 0)
        Nil
      else {
        val x = tile.x
        val y = tile.y
        val z = tile.zoom
        tile :: allTiles(Tile(x + x, y + y, z + 1), zoom - 1) ::: allTiles(Tile(x + x + 1, y + y, z + 1), zoom - 1) :::
          allTiles(Tile(x + x, y + y + 1, z + 1), zoom - 1) ::: allTiles(Tile(x + x + 1, y + y + 1, z + 1), zoom - 1)
      }
    }

    val tiles = allTiles(Tile(0, 0, 0), 4)

    for {(year, data) <- yearlyData
          tile <- tiles} {
      generateImage(year, tile, data)
    }
  }

}
