package ch.uzh.cl.slmlib.ngrams

import ch.uzh.cl.slmlib.ngrams.statistics.{RangeOrderStatistics, SingleOrderStatistics}
import org.apache.spark.rdd.RDD

/**
 * Set of implicit conversions.
 */
object Implicits {
  /**
   * Implicit conversion (union) of [[ch.uzh.cl.slmlib.ngrams.statistics.SingleOrderStatistics]] into <code>RDD[(NGram[T], U)]</code>.
   */
  implicit def SingleOrderStatistics2RDD[T, U](statistics: SingleOrderStatistics[T, U]): RDD[(NGram[T], U)] = statistics.rdd

  /**
    * Implicit conversion (union) of [[ch.uzh.cl.slmlib.ngrams.statistics.RangeOrderStatistics]] into <code>RDD[(NGram[T], U)]</code>.
    */
  implicit def RangeOrderStatistics2RDD[T, U](statistics: RangeOrderStatistics[T, U]): RDD[(NGram[T], U)] = {
    {
      for {
        order <- statistics.minOrder to statistics.maxOrder by 1
      } yield {
        statistics.mStatistics(order)
      }
    }.reduceLeft(_ ++ _)
  }


  /** Implicit conversion of [[ch.uzh.cl.slmlib.ngrams.NGramFilter]] into <code>(NGram[T],U) => Boolean</code>. */
  implicit def NGramFilter2Boolean[T, U](filter: NGramFilter[T, U]): ((NGram[T], U)) => Boolean = filter.filterFun

  /** Implicit conversion of [[ch.uzh.cl.slmlib.ngrams.NGram]] into a sequence <code>Seq[T]</code>. N-gram unwrapping. */
  implicit def NGram2Seq[T](ngram: NGram[T]): Seq[T] = ngram.sequence
}
