import me.lemire.integercompression.differential.IntegratedIntCompressor
import scala.util.Random

val iic = new IntegratedIntCompressor()

val data = Array[Int](1, 2, 3, 4, 5, 100, 120, 123, 150, 121, 122, 123, 125, 1000, 1100)
val data2 = (for (i <- 0 until 100) yield Random.nextInt).toArray

val compressed = iic.compress(data2)
compressed.size

//iic.uncompress(compressed)

