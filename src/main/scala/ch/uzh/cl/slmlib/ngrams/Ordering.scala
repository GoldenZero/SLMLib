package ch.uzh.cl.slmlib.ngrams

/**
 * N-gram statistics ordering factories.
 */
object Orderings {
  /** N-gram order based ordering. e.g. (NGram(List("the")),1) < (NGram(List("a","cat")),1) */
  def orderOrdering[T, U]: Ordering[(NGram[T], U)] = new {} with Ordering[(NGram[T], U)] {
    def compare(a: (NGram[T], U), b: (NGram[T], U)) = a._1.n compare b._1.n
  }

  /** Sequence based ordering. e.g. <code>(NGram(List("a","cat")),3) < (NGram(List("the","cat")),1)</code>. */
  def sequenceOrdering[T, U]: Ordering[(NGram[T], U)] = new {} with Ordering[(NGram[T], U)] {
    def compare(a: (NGram[T], U), b: (NGram[T], U)) = a._1.toString compare b._1.toString
  }

  /** Frequency based ordering. e.g. <code>(NGram(List("the","cat")),1) < (NGram(List("a","cat")),3)</code>. */
  def frequencyOrdering[T]: Ordering[(NGram[T], Int)] = new {} with Ordering[(NGram[T], Int)] {
    def compare(a: (NGram[T], Int), b: (NGram[T], Int)) = a._2 compare b._2
  }

  /** Probability based ordering. e.g. <code>(NGram(List("the","cat")),0.33) < (NGram(List("a","cat")),0.5)</code>. */
  def probabilityOrdering[T]: Ordering[(NGram[T], Double)] = new {} with Ordering[(NGram[T], Double)] {
    def compare(a: (NGram[T], Double), b: (NGram[T], Double)) = a._2 compare b._2
  }

  /** (K)ey (W)ord (I)n (C)ontext ordering, based on reversed prefix of KWIC. */
  def kwicOrderingLeft[T, U](keyPhrase: List[T]): Ordering[(NGram[T], U)] = new {} with Ordering[(NGram[T], U)] {
    def compare(a: (NGram[T], U), b: (NGram[T], U)) = {
      val idx_a = a._1.sequence.indexOfSlice(keyPhrase)
      // split the sequence into 2 parts: before/after the key word
      val split_a = a._1.sequence.splitAt(idx_a)
      val idx_b = b._1.sequence.indexOfSlice(keyPhrase)
      // split the sequence into 2 parts: before/after the key word
      val split_b = b._1.sequence.splitAt(idx_b)
      split_a._1.reverse.mkString(" ") compare split_b._1.reverse.mkString(" ")
    }
  }

  /** (K)ey (W)ord (I)n (C)ontext ordering, based on suffix of KWIC. */
  def kwicOrderingRight[T, U](keyPhrase: List[T]): Ordering[(NGram[T], U)] = new {} with Ordering[(NGram[T], U)] {
    def compare(a: (NGram[T], U), b: (NGram[T], U)) = {
      val idx_a = a._1.sequence.indexOfSlice(keyPhrase)
      // split the sequence into 2 parts: before/after the key word
      val split_a = a._1.sequence.splitAt(idx_a)
      val idx_b = b._1.sequence.indexOfSlice(keyPhrase)
      // split the sequence into 2 parts: before/after the key word
      val split_b = b._1.sequence.splitAt(idx_b)
      split_a._2.mkString(" ") compare split_b._2.mkString(" ")
    }
  }
}
