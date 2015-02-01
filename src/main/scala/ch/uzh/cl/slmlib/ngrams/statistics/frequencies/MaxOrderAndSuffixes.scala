package ch.uzh.cl.slmlib.ngrams.statistics.frequencies

import ch.uzh.cl.slmlib.SLM
import ch.uzh.cl.slmlib.ngrams.{NGram, PrefixPartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.HashMap

/**
 * Default frequencies calculation method. Maximum order n-grams are generated and aggregated.
 * Lower order n-grams frequencies can be derived as prefix of higher order n-grams frequencies.
 * Additionally, at the end of sequences, suffixes (lower order n-grams) have to be generated as these are no prefixes of higher order n-gram in the sequence.
 */
class MaxOrderAndSuffixes[T](lines: RDD[Seq[T]], val minOrder: Int = 1, val maxOrder: Int, minFrequency: Int = 1, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER) extends Frequencies[T] {
  private val suffixes = lines
    .flatMap(line => extend(SLM.getNGramsIterator(line, maxOrder, true)))
    .map(ngram => (ngram, 1))
    .reduceByKey(new PrefixPartitioner[T](lines.partitions.length, prefixLength = 2), _ + _).persist(StorageLevel.MEMORY_ONLY_SER)


  private[slmlib] val mStatistics = new HashMap[Int, RDD[(NGram[T], Int)]]()

  // highest order frequencies - just filter out all lower order ngrams
  mStatistics.put(maxOrder, suffixes.filter { case (ngram, freq) => ngram.order == maxOrder && freq >= minFrequency}.persist(storageLevel))

  // lower order frequencies are calculated by truncating longer ngrams to prefix of given length and csumming up the counts
  for {
    order <- maxOrder - 1 to minOrder by -1
  } yield {
    // are partitions preserved?
    val preserved: Boolean = (order > 1)
    // n-gram prefix for partitioning (unigrams probabilities will be partitioned on 1st word only after this)
    val prefixLength = {
      if (order > 1) 2 else 1
    }

    val orderNGrams = suffixes.filter { case (ngram, freq) => ngram.order >= order}
      .mapPartitions(_.map { case (ngram, freq) => (ngram.prefix(order), freq)}, preserved)
      .reduceByKey(new PrefixPartitioner[T](lines.partitions.length, prefixLength), _ + _)
      .filter { case (ngram, freq) => ngram.order == order && freq >= minFrequency}
      .persist(storageLevel)
    mStatistics.put(order, orderNGrams)
  }

  override val partitionsNumber: Int = lines.partitions.length


  // extends n-gram iterator with suffixes
  private def extend[T](a: Iterator[NGram[T]]): Seq[NGram[T]] = {
    val aux = a.toVector
    aux ++ (aux.last.suffixes.filter(_.size >= minOrder))
  }


}

/**
 * Factory to create [[MaxOrderAndSuffixes]] object instance.
 */
object MaxOrderAndSuffixesFactory extends FrequenciesFactory {
  override def getFrequencies[T](lines: RDD[Seq[T]], minOrder: Int, maxOrder: Int, minFrequency: Int, storageLevel: StorageLevel): Frequencies[T] =
    new MaxOrderAndSuffixes[T](lines, minOrder, maxOrder, minFrequency, storageLevel)
}
