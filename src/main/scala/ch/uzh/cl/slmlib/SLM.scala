package ch.uzh.cl.slmlib

import ch.uzh.cl.slmlib.ngrams.NGram
import ch.uzh.cl.slmlib.ngrams.statistics.frequencies._
import ch.uzh.cl.slmlib.ngrams.statistics.probabilities._
import ch.uzh.cl.slmlib.tokenize._
import org.apache.spark.Logging
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
 * Language model built from 3 components: [[tokenize.Tokenizer]], [[ngrams.statistics.frequencies.Frequencies]] and [[ngrams.statistics.probabilities.Probabilities]].
 * {{{
 * val filePath = "hdfs://MasterNode:8020/michal/wiki_extract_100000.txt" // input file path in HDFS
 * val lines = sc.textFile(filePath,8)                                    // rdd created from file in HDFS partitioned into 8 chunks
 *
 * // language model of n-grams of orders in range 1..7 and minimum 1 occurrence in the input file:
 * val slm :SLM[String] = new SLM(lines,1,7,1)
 * }}}
 * @param lines input lines, stored as RDD in distributed file system.
 * @param minOrder - Language model minimum order,
 * @param maxOrder - Language model maximum order.
 * @param minFrequency - Minimum frequency of n-grams.
 * @param tokenizer - Tokenizer that splits input lines into tokens. Optionally also annotating the input.
 * @param tokenizerStorageLevel - Storage level for tokenized input.
 * @param freqFactory - Factory for creating n-grams frequencies generation object instance.
 * @param freqStorageLevel - Storage level for generated frequencies.
 * @param probFactory - Factory for creating n-grams probabilities generation object instance.
 * @param probStorageLevel - Storage level for generated probabilities.
 */
class SLM[U](lines: RDD[String], minOrder: Int, maxOrder: Int, minFrequency: Int = 1, val tokenizer: Tokenizer[U] = Tokenizer.defaultTokenizer,tokenizerStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,  freqFactory: FrequenciesFactory = MaxOrderAndSuffixesFactory, freqStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER,probFactory: ProbabilitiesFactory = new NaiveBackoffFactory, probStorageLevel: StorageLevel = StorageLevel.NONE) extends Serializable with Logging {
  private val tokenizedLines = lines.mapPartitions(tokenizer.tokenize).persist(tokenizerStorageLevel)

  /** N-grams frequencies. */
  val frequencies = freqFactory.getFrequencies(tokenizedLines, minOrder, maxOrder, minFrequency, freqStorageLevel)

  /** N-grams probabilities. */
  lazy val probabilities = probFactory.getProbabilities(frequencies,probStorageLevel)

  logInfo("Initialized Language Model: minOrder=%d maxOrder=%d minFrequency=%d tokenizer=%s tokenizerStorageLevel=\"%s\" freqFactory=%s freqStorageLevel=\"%s\" probFactory=%s probStorageLevel=\"%s\"".format(minOrder, maxOrder, minFrequency,tokenizer.getClass.getCanonicalName, tokenizerStorageLevel.description,freqFactory.getClass.getCanonicalName,freqStorageLevel.description,probFactory.getClass.getCanonicalName,probStorageLevel.description))
}




private[slmlib] object SLM {
  // n-grams of n size
  def getNGramsRDDWithStats[T](lines: RDD[String], n: Int, tokenizer: Tokenizer[T] = Tokenizer.defaultTokenizer): RDD[(NGram[T], Int)] =
    lines.mapPartitions(tokenizer.tokenize).flatMap(line => getNGramsIterator(line, n)).map(ngram => (ngram, 1)).reduceByKey(_ + _)

  /**
   * main function to produce n-grams of length n
   * @param tokens tokenized input, most often a tokenized input line
   * @param n length of n-grams generated
   * @tparam T type of n-gram eg. String - token, (String,String) - (token,POS)
   * @return iterator over generated n-grams
   */
  def getNGramsIterator[T](tokens: Seq[T], n: Int, partial: Boolean = false): Iterator[NGram[T]] = {
    // n-long sliding window iteration, withPartial(boolean) prevents shorther n-grams in case n is higher than the number of tokens
    tokens.iterator.sliding(n).withPartial(partial).map(slice => new NGram(slice))
  }

  // n-grams of size between n_min and n_max
  def getNGramsRDDWithStats[T](lines: RDD[String], n_min: Int, n_max: Int, tokenizer: Tokenizer[T] = Tokenizer.defaultTokenizer): RDD[(NGram[T], Int)] =
    lines.mapPartitions(tokenizer.tokenize).flatMap(line => getNGramsIterator(line, n_min, n_max)).map(ngram => (ngram, 1)).reduceByKey(_ + _)

  /**
   * produces n-grams of length between n_min and n_max
   * @param tokens tokenized input, most often a tokenized input line
   * @param n_min minimum length of n-grams generated
   * @param n_max maximum length of n-grams generated
   * @tparam T type of n-gram eg. String - token, (String,String) - (token,POS)
  @return iterator over generated n-grams
   */
  def getNGramsIterator[T](tokens: Seq[T], n_min: Int, n_max: Int, partial: Boolean = false): Iterator[NGram[T]] = {
    require(n_min > 0, "minimum order of N-gram must be > 0")
    require(n_min <= n_max, "minimum order must be <=maximum order")
    // n-long sliding window iteration, withPartial prevents shorther n-grams in case n is higher than the number of tokens
    (
      for {
        n <- n_min to n_max
      } yield getNGramsIterator(tokens, n, partial)
      ).reduceLeft(_ ++ _)
  }

  def getNGramsSeqWithStats[T](lines: Seq[String], n: Int, tokenizer: Tokenizer[T] = Tokenizer.defaultTokenizer, partial: Boolean = false): Seq[(NGram[T], Int)] =
    tokenizer.tokenize(lines.iterator).flatMap(line => getNGramsIterator(line, n,true)).toVector.map(ngram => (ngram, 1)).groupBy(_._1).mapValues(_.length).toVector
}
