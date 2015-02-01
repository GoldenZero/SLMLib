package ch.uzh.cl.slmlib.ngrams

import org.apache.spark.Partitioner

/**
 * Customizes collections partitioning so NGrams with the same prefix are shuffled to the same partitions.
 * Derived from <code>org.apache.spark.HashPartitioner</code>.
 * @param partitions number of partitions
 * @param prefixLength number of prefix tokens
 */
class PrefixPartitioner[T](partitions: Int,prefixLength:Int) extends Partitioner {
  /**
   * maps a key to a partition
   */
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => mod(key.asInstanceOf[NGram[T]].prefix(prefixLength).toString.hashCode, numPartitions)
  }

  /**
   * custom modulo function that handles minus values
   */
  private def mod(x: Int, mod: Int): Int = {
    val aux = x % mod
    if (aux < 0) aux + mod else aux
  }

  /**
   * comparator function, it is used by Spark to decide if 2 RDDs are using the same partitioner
   */
  override def equals(other: Any): Boolean = other match {
    case h: PrefixPartitioner[T] =>
      h.numPartitions == numPartitions && h.prefixSize == prefixSize
    case _ =>
      false
  }

  def numPartitions = partitions
  def prefixSize = prefixLength

  override def hashCode: Int = numPartitions
}
