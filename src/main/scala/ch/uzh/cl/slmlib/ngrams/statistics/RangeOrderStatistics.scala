package ch.uzh.cl.slmlib.ngrams.statistics

import ch.uzh.cl.slmlib.ngrams.{NGram, NGramFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

/**
 * N-Gram collections with corresponding statistics (frequencies or probabilities) for n-grams in order range.
 */
trait RangeOrderStatistics[T, U] extends Statistics[T, U] {
  private[slmlib] val mStatistics: HashMap[Int, RDD[(NGram[T], U)]]

  /** Minimum order of n-grams in range. */
  val minOrder: Int

  /** Maximum order of n-grams in range. */
  val maxOrder: Int

  override def partitionsNumber = mStatistics(minOrder).partitions.length

  /** Returns statistics object with n-grams if given order. apply() function can be also called:
    * {{{
    * val a : RangeOrderStatistics
    * a(2)
    * }}}
    */
  def apply(order: Int): SingleOrderStatistics[T, U] = new {} with SingleOrderStatistics[T, U] {
    override val rdd: RDD[(NGram[T], U)] = RangeOrderStatistics.this.mStatistics(order)
  }

  /** Returns statistics object with n-grams in given order range. apply() function can be also called:
    * {{{
    * val a : RangeOrderStatistics
    * a(2,3)
    * }}}
    */
  def apply(orderMin: Int, orderMax: Int): RangeOrderStatistics[T, U] = new {} with RangeOrderStatistics[T, U] {
    require(orderMin >= RangeOrderStatistics.this.minOrder && orderMin <= RangeOrderStatistics.this.maxOrder && orderMax >= RangeOrderStatistics.this.minOrder && orderMax <= RangeOrderStatistics.this.maxOrder && orderMin < orderMax)
    override val minOrder = orderMin
    override val maxOrder = orderMax

    override val mStatistics = new HashMap[Int, RDD[(NGram[T], U)]]()
    for {
      order <- orderMin to orderMax by 1
    } yield {
      mStatistics(order) = RangeOrderStatistics.this.mStatistics(order)
    }
  }

  override def filter(ngFilter: NGramFilter[T, U]): RangeOrderStatistics[T, U] = new {} with RangeOrderStatistics[T, U] {
    override val minOrder = RangeOrderStatistics.this.minOrder
    override val maxOrder = RangeOrderStatistics.this.maxOrder

    override val mStatistics = new HashMap[Int, RDD[(NGram[T], U)]]()
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      mStatistics(order) = ngFilter.filter(RangeOrderStatistics.this.mStatistics(order))
    }
  }

  /** Memory efficient iterator over n-grams of all orders.
    * Iterator retrieves one partition at a time, so the memory required on client side equals to the size of the biggest partition.
    */
  override def iterator: Iterator[(NGram[T], U)] = {
    {
      for {
        order <- minOrder to maxOrder by 1
      } yield {
        mStatistics(order).toLocalIterator
      }
    }.reduceLeft(_ ++ _)
  }

  override def saveAsTextFile(path: String, format: String): Unit = {
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      val childPath = path + "/" + order
      mStatistics(order).mapPartitions(_.map(tuple => format.format(tuple._1, tuple._2))).saveAsTextFile(childPath)
    }
  }

  override def saveNGramsAsSerializedRDD(path: String): Unit = {
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      val childPath = path + "/" + order
      mStatistics(order).saveAsObjectFile(childPath)
    }
  }

  override def count: Long = {
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      mStatistics(order).count
    }
  }.reduceLeft(_ + _)

}
