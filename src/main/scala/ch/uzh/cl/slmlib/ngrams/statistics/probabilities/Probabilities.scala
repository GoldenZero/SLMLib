package ch.uzh.cl.slmlib.ngrams.statistics.probabilities

import ch.uzh.cl.slmlib.ngrams.statistics.RangeOrderStatistics
import ch.uzh.cl.slmlib.ngrams.statistics.frequencies.Frequencies
import ch.uzh.cl.slmlib.tokenize.Tokenizer
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel

/**
 * N-Gram collections with corresponding probabilities.
 */
trait Probabilities[T] extends RangeOrderStatistics[T, Double] {
  override def saveAsTextFile(path: String, format: String = "%s %.10f") = super.saveAsTextFile(path,format)
  //override def saveSortedAsTextFile(order:Int)(path: String, partitions: Int = partitionsNumber, format: String = "%s %.10f"): Unit = apply(order).sortByKey(true, partitions).map(tuple => format.format(tuple._1, tuple._2)).saveAsTextFile(path)

  /** Save sorted (key-based) statistics into distributed storage (e.g. HDFS, Amazon S3). */
  def saveSortedAsTextFile(path: String, partitions: Int = this.partitionsNumber, format:String= "%s %.10f"): Unit = {
    for {
      order <- minOrder to maxOrder by 1
    } yield {
      val childPath = path + "/" + order
      mStatistics(order).sortByKey(true, partitions).map(tuple => format.format(tuple._1, tuple._2)).saveAsTextFile(childPath)
    }
  }

  /**
   * Calculates probability score for a given string using tokenizer and precomputed probabilities.
   * Depending on the implementation, different approaches can be used to estimate unknown n-grams.
   */
  def probabilityScore(query: String, tokenizer: Tokenizer[T], logScale:Boolean = true): Double
}

/**
 * Factory object to create probabilities object.
 */
trait ProbabilitiesFactory extends Serializable {
  /**
   * creates probabilities object
   */
  def getProbabilities[T](frequencies: Frequencies[T], storageLevel: StorageLevel): Probabilities[T]
}
