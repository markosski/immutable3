package immutabledb.util

import scala.collection.mutable.BitSet
import scala.util.hashing.MurmurHash3

/**
  * Created by marcin on 4/22/16.
  * We are using one hashing algorithm applied multiple times on the output if needed.
  */
class Bloom(size: Int, prob: Double) extends Serializable {

    val bits = BitSet()

    lazy val hashIter: Int = math.max(1, (bitSize / size * math.log(2)).toInt)

    lazy val bitSize: Int = -(size * math.log(prob) / math.pow(math.log(2), 2)).toInt

    def hash(item: String): Int = {
        // TODO: Test if applying MurmurHash recursively on its own output still produces uniform values.
        math.abs(MurmurHash3.stringHash(item)) % bitSize
    }

    def hashList(item: String): List[Int] = {
        var hashed = hash(item) :: Nil
        for (i <- 0 until hashIter - 1) hashed = hash(hashed.mkString) :: hashed
        hashed
    }

    def add(item: String) = {
        hashList(item) foreach (x => bits += x)
    }

    def contains(item: String): Boolean = {
        hashList(item) forall (x => if (bits.contains(x)) true else false)
    }
}