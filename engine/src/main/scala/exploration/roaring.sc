import com.typesafe.scalalogging.Logger
import org.roaringbitmap.RoaringBitmap

val logger = Logger("main")
logger.info("creating bunch of ints")

val l = 10 until 2000000

val rr1 = RoaringBitmap.bitmapOf(l: _*)

val rr2 = RoaringBitmap.bitmapOf(-1,2, 1000000, 1000001)

RoaringBitmap.and(rr1, rr2)

