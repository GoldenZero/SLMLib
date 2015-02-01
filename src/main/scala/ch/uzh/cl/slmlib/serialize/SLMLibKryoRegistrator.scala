package ch.uzh.cl.slmlib.serialize

import ch.uzh.cl.slmlib.ngrams.NGram
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
 * Registers NGram classes with Kryo serializing library.
 */


class SLMLibKryoRegistrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[NGram[String]])
    kryo.register(classOf[NGram[(String, String)]])
    kryo.register(classOf[NGram[(String, String, String)]])
    kryo.register(classOf[NGram[Int]])
  }
}


