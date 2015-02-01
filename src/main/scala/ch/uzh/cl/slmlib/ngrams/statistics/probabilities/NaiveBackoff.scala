package ch.uzh.cl.slmlib.ngrams.statistics.probabilities

import ch.uzh.cl.slmlib.SLM
import ch.uzh.cl.slmlib.ngrams.Implicits._
import ch.uzh.cl.slmlib.ngrams.statistics.frequencies.Frequencies
import ch.uzh.cl.slmlib.ngrams.{NGram, NGramFilter}
import ch.uzh.cl.slmlib.tokenize.Tokenizer
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel

/**
 * Based on [[RawProbabilities]] method. For calculating probability scores of unknown n-grams it uses method called 'StupidBackoff'.
 * It backs off to lower order n-gram probability with an <code>alpha</code> weight,
 */
class NaiveBackoff[T](private val frequencies: Frequencies[T], private val alpha: Int => Double, storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER) extends RawProbabilities[T](frequencies, storageLevel) with Logging {


  override def probabilityScore(query: String, tokenizer: Tokenizer[T], logScale:Boolean = true): Double = {
    // initial timestamp
    val start_timestamp = System.currentTimeMillis()

    // tokenize the query
    val queryMap: Map[NGram[T], Int] = SLM.getNGramsSeqWithStats(List(query), maxOrder, tokenizer, true).toMap
    logInfo("query:'%s' query_ngrams:%s".format(query, queryMap))

    if (queryMap.size == 0) throw new RuntimeException("Empty query")

    // the order of the query - it doesn't make sense to search for bigrams in probabilities of 4-grams for instance
    val size = queryMap.head._1.order

    // the minimum frequency of the language model
    val min = frequencies.minOrder
    def aux(remaining: Map[NGram[T], Int], order: Int, probability: Double, alfa: Double): Double = order match {
      case `min` => {
        if (remaining.size > 0) {
          val slices = remaining.keys.map(_.sequence.toSeq).toSeq
          // the intersection of remaining n-grams in the query with probabilities RDD
          val intersect: Array[(NGram[T], Double)] = mStatistics(order).filter(NGramFilter.containsAnySlice[T, Double](slices)).collect
            .map { case (ngram, prob) => (ngram, math.pow(prob, remaining(ngram)))}

          // 1) remaining query ngrams
          val nRemaining: Map[NGram[T], Int] = remaining.filterNot(x => intersect.toMap.contains(x._1))

          // 3) probability
          val nProbability = probability * intersect.map(_._2).aggregate(1.0)((agg, prob) => agg * alfa * prob, _ * _)

          // unseen n-grams of lowest order
          val unseenUnigramsCount = nRemaining.map(_._2).aggregate(0)(_ + _, _ + _)
          logInfo("unseen_unigrams:%d".format(unseenUnigramsCount))

          // uniform distribution estimate of unseen unigrams
          val zeroProb = 1.0 / minOrderCount

          val finalProbability = logScale match {
            case true => math.log(nProbability * math.pow(alfa * zeroProb, unseenUnigramsCount))
            case false => nProbability * math.pow(alfa * zeroProb, unseenUnigramsCount)
          }
          val end_timestamp = System.currentTimeMillis()
          logInfo("query:'%s' probability:%.10f elapsed_time:%d sec.".format(query, finalProbability, (end_timestamp - start_timestamp) / 1000))
          finalProbability
        } else {
          val finalProbability = logScale match {
            case true => math.log(probability)
            case false => probability
          }
          val end_timestamp = System.currentTimeMillis()
          logInfo("query:'%s' probability:%.10f elapsed_time:%d sec.".format(query, finalProbability, (end_timestamp - start_timestamp) / 1000))
          finalProbability
        }

      }
      case _ => {
        if (remaining.size > 0) {
          val slices = remaining.keys.map(_.sequence.toSeq).toSeq

          // the intersection of remaining n-grams in the query with probabilities RDD
          val intersect: Array[(NGram[T], Double)] = mStatistics(order).filter(NGramFilter.containsAnySlice[T, Double](slices)).collect
            .map { case (ngram, prob) => (ngram, math.pow(prob, remaining(ngram)))}

          // update the values:
          // 1) remaining query ngrams truncated to lower level ngrams (first token discarded)
          val nRemaining: Map[NGram[T], Int] = remaining.filterNot(x => intersect.toMap.contains(x._1)).map { case (ngram, cnt) => (ngram.tail, cnt)}.filter { case (ngram, freq) => ngram.size > 0}.toVector.groupBy(_._1).mapValues(_.map(_._2).reduce(_ + _))

          // 2) backoff alpha for lower level n-gram
          val nAlpha = alfa * alpha(order - 1)

          // 3) probability
          val nProbability = probability * intersect.map(_._2).aggregate(1.0)((agg, prob) => agg * alfa * prob, _ * _)

          // backoff to lower level ngram:
          aux(nRemaining, order - 1, nProbability, nAlpha)
        } else {
          val finalProbability = logScale match {
            case true => math.log(probability)
            case false => probability
          }
          val end_timestamp = System.currentTimeMillis()
          logInfo("query:'%s' probability:%.10f elapsed_time:%d sec.".format(query, finalProbability, (end_timestamp - start_timestamp) / 1000))
          finalProbability
        }
      }
    }
    aux(queryMap, math.min(maxOrder, size), 1.0, 1.0)
  }
}

/**
 * Creates an instance object of [[NaiveBackoff]] with <code>alpha</code>.
 * @param alpha n-gram order dependent weight used for back off. Default is set to 0.4 as in original paper and it's n-gram order independent.
 */
class NaiveBackoffFactory(alpha: Int => Double = ((order: Int) => 0.4)) extends ProbabilitiesFactory {
  override def getProbabilities[T](frequencies: Frequencies[T], storageLevel: StorageLevel): Probabilities[T] = new NaiveBackoff[T](frequencies, alpha, storageLevel)
}
