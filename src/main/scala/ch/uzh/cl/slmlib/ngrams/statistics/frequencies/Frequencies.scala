package ch.uzh.cl.slmlib.ngrams.statistics.frequencies

import ch.uzh.cl.slmlib.ngrams.statistics.RangeOrderStatistics
import ch.uzh.cl.slmlib.tokenize.Tokenizer
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
/**
 * N-Gram collections with corresponding frequencies.
 */
trait Frequencies[T] extends RangeOrderStatistics[T, Int] {
  override def saveAsTextFile(path: String, format: String = "%s %d") = super.saveAsTextFile(path,format)

  /** Save sorted (key-based) frequencies into distributed storage (e.g. HDFS, Amazon S3). */
  def saveSortedAsTextFile(path: String, partitions: Int = this.partitionsNumber, format:String= "%s %d"): Unit = {
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      val childPath = path + "/" + order
      mStatistics(order).sortByKey(true, partitions).map(tuple => format.format(tuple._1, tuple._2)).saveAsTextFile(childPath)
    }
  }
}

/**
 * Factory object to create frequencies object.
 */
trait FrequenciesFactory extends Serializable {
  /**
   * creates frequencies object
   */
  def getFrequencies[T](lines: RDD[Seq[T]], minOrder:Int, maxOrder: Int, minFrequency: Int = 1, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER): Frequencies[T]
}