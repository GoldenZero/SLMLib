package ch.uzh.cl.slmlib.ngrams.statistics

import ch.uzh.cl.slmlib.ngrams.{NGramFilter, NGram}

/**
 * N-Gram collections with corresponding statistics ([[frequencies]] or [[probabilities]]).
 */
trait Statistics[T,U] extends Serializable {
  /** Number of partitions in collection's RDD. */
  def partitionsNumber:Int

  /** Iterator over the entire statistics collection. */
  def iterator:Iterator[(NGram[T],U)]

  /** Number of n-grams in the collection. */
  def count:Long

  /** Save statistics into distributed storage (e.g. HDFS, Amazon S3). */
  def saveAsTextFile(path: String, format: String ): Unit

  /** Save serialized statistics into distributed storage (e.g. HDFS, Amazon S3). */
  def saveNGramsAsSerializedRDD(path: String): Unit

  /** Filter the statistics. */
  def filter(ngFilter: NGramFilter[T, U]): Statistics[T,U]


}
