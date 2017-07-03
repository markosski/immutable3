package exploration

import com.typesafe.scalalogging.Logger
import org.roaringbitmap.RoaringBitmap
import org.scalameter._

/**
  * Created by marcin on 12/25/16.
  */
object Roaring extends App {
    val logger = Logger("main")
    logger.debug("creating bunch of ints")

    val l1 = 10 until 200000000
    val l2 = 1000 until 10000000

    val rr1 = RoaringBitmap.bitmapOf(l1: _*)

//    val rr2 = RoaringBitmap.bitmapOf(-1,2, 500, 321345, 1000000, 1000001)
    val rr2 = RoaringBitmap.bitmapOf(l2: _*)

    val time = withWarmer(new Warmer.Default) measure {
        logger.debug("intersetion")
        val res = RoaringBitmap.or(rr1, rr2)

        logger.debug("done")
        println(res.getSizeInBytes)
    }

    println(time)

}
