package observatory

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CapstoneSuite
  extends Visualization2Test
    with ExtractionTest
    with VisualizationTest
    with InteractionTest
    with ManipulationTest
    with Interaction2Test

