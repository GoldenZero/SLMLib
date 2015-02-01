package ch.uzh.cl.slmlib.ngrams.statistics.probabilities

import ch.uzh.cl.slmlib.SLM
import ch.uzh.cl.slmlib.ngrams.statistics.frequencies.Frequencies
import ch.uzh.cl.slmlib.ngrams.{PrefixPartitioner, NGramFilter, NGram}
import ch.uzh.cl.slmlib.ngrams.NGramFilter._
import SLM._
import ch.uzh.cl.slmlib.tokenize.Tokenizer
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import ch.uzh.cl.slmlib.ngrams.Implicits._

import scala.collection.mutable.HashMap

import org.apache.spark.Logging

/**
 * Basic method, Divide frequencies of n-gram by frequency of lower order prefix. For lowest order n-grams divide frequency by number of all n-grams of the lowest order.
 */
class RawProbabilities[T](private val frequencies: Frequencies[T], storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER) extends Probabilities[T]  {
  private[slmlib] val mStatistics = new HashMap[Int, RDD[(NGram[T], Double)]]()
  private[probabilities] val minOrderCount = frequencies(frequencies.minOrder).aggregate(0)((sum, freq) => sum + freq._2, _ + _)

  // for lowest order frequencies, normalize by the total number of tokens
  mStatistics.put(frequencies.minOrder, frequencies(frequencies.minOrder).mapPartitions(_.map{case (ngram,freq) => (ngram,freq.toDouble / minOrderCount)}).persist(storageLevel))

  // as the different order ngrams are customly partitioned based on the first token we can avoid shuffling the data by applying map functions per partition
  private def divideFreqsAndMapBack(entries: Iterator[(NGram[T], ((NGram[T], Int), Int))]): Iterator[(NGram[T], Double)] = entries.map { case (init, ((last, freq), freqLower)) => (init + last, freq.toDouble / freqLower)}
  private def mapToPrefix(entries: Iterator[(NGram[T], Int)]): Iterator[(NGram[T], (NGram[T], Int))] = entries.map { case (ngram, freq) => (ngram.init, (ngram.last, freq))}

  for {
    order <- frequencies.minOrder+1 to frequencies.maxOrder
  } yield {
    // are partitions preserved?
    val preserved:Boolean = (order > 2)
    // n-gram prefix for partitioning (bigrams probabilities will be partitioned on 1st word only after this)
    val prefixLength = {if (order > 2) 2 else 1}

    val orderNGrams = frequencies(order)
    val lowerOrderNGrams = frequencies(order - 1)

    mStatistics.put(order, orderNGrams.mapPartitions(mapToPrefix, preserved)
      .join(lowerOrderNGrams,new PrefixPartitioner[T](frequencies.partitionsNumber,prefixLength))
      .mapPartitions(divideFreqsAndMapBack, true)
      .persist(storageLevel))
  }

  override val maxOrder = frequencies.maxOrder
  override val minOrder = frequencies.minOrder





  override def probabilityScore(query: String,tokenizer:Tokenizer[T], logScale:Boolean = true): Double= ???

  override val partitionsNumber: Int = frequencies.partitionsNumber
}

/**
 * Create an instance object of [[RawProbabilities]]
 */
object RawProbabilitiesFactory extends ProbabilitiesFactory {
  override def getProbabilities[T](frequencies: Frequencies[T],storageLevel: StorageLevel) = new RawProbabilities[T](frequencies,storageLevel)
}