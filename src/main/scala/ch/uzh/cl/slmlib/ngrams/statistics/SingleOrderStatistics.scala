package ch.uzh.cl.slmlib.ngrams.statistics

import ch.uzh.cl.slmlib.ngrams.{NGram, NGramFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * N-Gram collections with corresponding statistics (frequencies or probabilities) for n-grams with given order.
 */
trait SingleOrderStatistics[T, U] extends Statistics[T, U] {
  private[slmlib] val rdd: RDD[(NGram[T], U)]

  override def partitionsNumber: Int = rdd.partitions.length

  /** Memory efficient iterator over all n-grams.
    * Iterator retrieves one partition at a time, so the memory required on client side equals to the size of the biggest partition.
    */
  override def iterator: Iterator[(NGram[T], U)] = rdd.toLocalIterator

  override def saveAsTextFile(path: String, format: String): Unit =
    rdd.mapPartitions(_.map(tuple => format.format(tuple._1, tuple._2))).saveAsTextFile(path)

  override def saveNGramsAsSerializedRDD(path: String): Unit = rdd.saveAsObjectFile(path)

  override def filter(ngFilter: NGramFilter[T, U]): SingleOrderStatistics[T, U] = new {} with SingleOrderStatistics[T, U] {
    val rdd: RDD[(NGram[T], U)] = ngFilter.filter(SingleOrderStatistics.this.rdd)
  }

  override def count: Long = rdd.count
}
