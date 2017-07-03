package immutabledb.codec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import immutabledb.DataType
import java.nio.ByteBuffer

import scala.reflect.ClassTag
import org.iq80.snappy._

/**
  * Created by marcin1 on 7/28/17.
  */
//class SnappyCodec[T <: DataType](val dtype: T)(implicit tag: ClassTag[T#A]) extends Codec[T] {
//
//    def encode(values: Array[String]): ByteBuffer = {
//        val os = new ByteArrayOutputStream()
//        val snapOS = new ByteArrayOutputStream()
//
//        val snap = new SnappyOutputStream(snapOS)
//
//        for (i <- values.indices) {
//            os.write(dtype.stringToBytes(values(i)))
//        }
//
//        snap.write(os.toByteArray)
//        snap.flush
//
//        ByteBuffer.wrap(snapOS.toByteArray)
//    }
//
//    def decode(dataSize: Int, data: ByteBuffer): Array[T#A] = {
//        val snap = new SnappyInputStream(
//            new ByteArrayInputStream(data.array)
//        )
//
//        val dataOut = new Array[T#A](dataSize)
//
//        val bytes = new Array[Byte](dtype.size)
//        var i = 0
//        while (snap.read(bytes) != -1) {
//            println(bytes.toList)
//            dataOut(i) = dtype.bytesToValue(bytes)
//            i += 1
//        }
//        dataOut
//    }
//}
