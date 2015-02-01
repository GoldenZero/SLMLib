package ch.uzh.cl.slmlib.ngrams

import org.apache.spark.rdd.RDD



/**
 * Wraps filter functions that can be applied on NGram statistics.
 */
trait  NGramFilter[T, U] extends Serializable {
  /** Applies filter function on statistics collection. */
  final def filter(statistics: RDD[(NGram[T], U)]): RDD[(NGram[T], U)] = statistics.filter(filterFun)

  /** Filter function definition. */
  def filterFun(ngram: (NGram[T], U)): Boolean

  /** Conjunction of two filters. */
  final def &&(that:NGramFilter[T, U]): NGramFilter[T, U] = new {} with NGramFilter[T, U] {
    override def filterFun(ngram: (NGram[T], U)): Boolean = NGramFilter.this.filterFun(ngram) && that.filterFun(ngram)
  }

  /** Disjunction of two filters. */
  final def ||(that:NGramFilter[T, U]): NGramFilter[T, U] = new {} with NGramFilter[T, U] {
    override def filterFun(ngram: (NGram[T], U)): Boolean = NGramFilter.this.filterFun(ngram) || that.filterFun(ngram)

  }
}

/** Predefined filters. */
object NGramFilter {

  /** Equality of the key with provided NGram. */
  def equals[T, U](that: NGram[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1 == that
    }
  }

  /** Frequency range filter. */
  def hasFrequencyRange[T, Int](lowerBound: Int, upperBound: Int = Integer.MAX_VALUE)(implicit ord: Ordering[Int]) = {
    new {} with NGramFilter[T, Int] {
      override def filterFun(ngram: (NGram[T], Int)): Boolean = ord.gteq(ngram._2, lowerBound) && ord.lteq(ngram._2, upperBound)
    }
  }

  /** Probability range filter. */
  def hasProbabilityRange[T, Float](lowerBound: Float, upperBound: Float = 1.0)(implicit ord: Ordering[Float]) = {
    new {} with NGramFilter[T, Float] {
      override def filterFun(ngram: (NGram[T], Float)): Boolean = ord.gteq(ngram._2, lowerBound) && ord.lteq(ngram._2, upperBound)
    }
  }

  /** N-gram single token suffix matching. */
  def endsWith[T, U](token: T) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.endsWith(token)
    }
  }

  /** N-gram suffix matching. */
  def endsWithSlice[T, U](slice: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.endsWithSlice(slice)
    }
  }

  /** N-gram single token prefix matching. */
  def startsWith[T, U](token: T) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.startsWith(token)
    }
  }

  /** N-gram prefix matching. */
  def startsWithSlice[T, U](slice: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.startsWithSlice(slice)
    }
  }

  /** Single token matching the pattern. */
  def contains[T, U](token: T) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.contains(token)
    }
  }

  /** Sequence of tokens matching the pattern. */
  def containsSlice[T, U](slice: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsSlice(slice)
    }
  }

  /** Single token matching the pattern at specific position. */
  def containsAt[T, U](index: Int)(token: T) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAt(index)(token)
    }
  }

  /** Sequence of tokens matching the pattern starting at specific position. */
  def containsSliceAt[T, U](index: Int)(slice: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsSliceAt(index)(slice)
    }
  }

  /** All single tokens matching the pattern. */
  def containsAll[T, U](tokens: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAll(tokens)
    }
  }

  /** All sequences of tokens matching the pattern. */
  def containsAllSlices[T, U](slices: Seq[Seq[T]]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAllSlices(slices)
    }
  }

  /** Any of single tokens matching the pattern. */
  def containsAny[T, U](tokens: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAny(tokens)
    }
  }

  /** Any of sequences of tokens matching the pattern. */
  def containsAnySlice[T, U](slices: Seq[Seq[T]]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAnySlice(slices)
    }
  }

  /** Any of single tokens matching the pattern at specific position. */
  def containsAnyAt[T, U](index: Int)(tokens: Seq[T]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAnyAt(index)(tokens)
    }
  }

  /** Any of sequences of tokens matching the pattern starting at specific position. */
  def containsAnySliceAt[T, U](index: Int)(slices: Seq[Seq[T]]) = {
    new {} with NGramFilter[T, U] {
      override def filterFun(ngram: (NGram[T], U)): Boolean = ngram._1.containsAnySliceAt(index)(slices)
    }
  }
}