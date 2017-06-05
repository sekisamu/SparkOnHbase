package cn.andlinks.cdhBased.cdhBased.Utils

/**
  * Created by hammer on 2017/5/16.
  */
import java.io._
import java.util.Random

import cn.andlinks.cdhBased.cdhBased.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}

/** CallSite represents a place in user code. It can have a short and a long form. */
case class CallSite(shortForm: String, longForm: String)

object CallSite {
  val SHORT_FORM = "callSite.short"
  val LONG_FORM = "callSite.long"
}

/**
  * Various utility methods used by Spark.
  */
object Utils extends Logging {
  val random = new Random()

  /** Serialize an object using Java serialization */
  def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /** Deserialize an object using Java serialization */
  def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize an object using Java serialization and the given ClassLoader */
  def deserialize[T](bytes: Array[Byte], loader: ClassLoader): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis) {
      override def resolveClass(desc: ObjectStreamClass) =
        Class.forName(desc.getName, false, loader)
    }
    ois.readObject.asInstanceOf[T]
  }

  /** Deserialize a Long value (used for [[org.apache.spark.api.python.PythonPartitioner]]) */
  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    // Note: we assume that we are given a Long value encoded in network (big-endian) byte order
    var result = bytes(7) & 0xFFL
    result = result + ((bytes(6) & 0xFFL) << 8)
    result = result + ((bytes(5) & 0xFFL) << 16)
    result = result + ((bytes(4) & 0xFFL) << 24)
    result = result + ((bytes(3) & 0xFFL) << 32)
    result = result + ((bytes(2) & 0xFFL) << 40)
    result = result + ((bytes(1) & 0xFFL) << 48)
    result + ((bytes(0) & 0xFFL) << 56)
  }

  /** Serialize via nested stream using specific serializer */
  def serializeViaNestedStream(os: OutputStream, ser: SerializerInstance)(
    f: SerializationStream => Unit) = {
    val osWrapper = ser.serializeStream(new OutputStream {
      def write(b: Int) = os.write(b)

      override def write(b: Array[Byte], off: Int, len: Int) = os.write(b, off, len)
    })
    try {
      f(osWrapper)
    } finally {
      osWrapper.close()
    }
  }

  /** Deserialize via nested stream using specific serializer */
  def deserializeViaNestedStream(is: InputStream, ser: SerializerInstance)(
    f: DeserializationStream => Unit) = {
    val isWrapper = ser.deserializeStream(new InputStream {
      def read(): Int = is.read()

      override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
    })
    try {
      f(isWrapper)
    } finally {
      isWrapper.close()
    }
  }

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader


}