package immutabledb

object Conversions {
    def bytesToLong(bytes: Array[Byte]): Long = {
        var result = 0
        result = result + (bytes(7) & 0xFF) << 8
        result = result + (bytes(6) & 0xFF) << 8
        result = result + (bytes(5) & 0xFF) << 8
        result = result + (bytes(4) & 0xFF) << 8
        result = result + (bytes(3) & 0xFF) << 8
        result = result + (bytes(2) & 0xFF) << 8
        result = result + (bytes(1) & 0xFF) << 8
        result = result + (bytes(0) & 0xFF)
        result.asInstanceOf[Long]
    }

    def bytesToInt(bytes: Array[Byte]): Int = {
        var result = 0
        result = result + (bytes(3) & 0xFF) << 8
        result = result + (bytes(2) & 0xFF) << 8
        result = result + (bytes(1) & 0xFF) << 8
        result = result + (bytes(0) & 0xFF)
        result
    }

    def bytesToShort(bytes: Array[Byte]): Short = {
        var result = 0
        result = result + (bytes(1) & 0xFF) << 8
        result = result + (bytes(0) & 0xFF)
        result.asInstanceOf[Short]
    }

    def intToBytes(intVal: Int): Array[Byte] = {
        val bytes = new Array[Byte](4)
        bytes(3) = ((intVal >> 24) & 0xFF).toByte
        bytes(2) = ((intVal >> 16) & 0xFF).toByte
        bytes(1) = ((intVal >> 8) & 0xFF).toByte
        bytes(0) = (intVal & 0xFF).toByte
        bytes
    }

    def shortToBytes(intVal: Short): Array[Byte] = {
        val bytes = new Array[Byte](2)
        bytes(1) = ((intVal >> 8) & 0xFF).toByte
        bytes(0) = (intVal & 0xFF).toByte
        bytes
    }

    def longToBytes(intVal: Long): Array[Byte] = {
        val bytes = new Array[Byte](8)
        bytes(7) = ((intVal >> 56) & 0xFF).toByte
        bytes(6) = ((intVal >> 48) & 0xFF).toByte
        bytes(5) = ((intVal >> 32) & 0xFF).toByte
        bytes(4) = ((intVal >> 24) & 0xFF).toByte
        bytes(3) = ((intVal >> 24) & 0xFF).toByte
        bytes(2) = ((intVal >> 16) & 0xFF).toByte
        bytes(1) = ((intVal >> 8) & 0xFF).toByte
        bytes(0) = (intVal & 0xFF).toByte
        bytes
    }
}
