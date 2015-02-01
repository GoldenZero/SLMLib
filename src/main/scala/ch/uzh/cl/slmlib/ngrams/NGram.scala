package ch.uzh.cl.slmlib.ngrams

/**
 * Represents n-gram object.
 **/

case class NGram[T](val sequence: Seq[T]) extends Ordered[NGram[T]] {
  def this(ngram: Seq[T], n: Int) = {
    this(ngram)
    require(ngram.length == n, "numbers of tokens in %d-gram cannot be %d".format(n, ngram.length))
  }

  /** Concatenates two n-grams. */
  def +(that: NGram[T]): NGram[T] = NGram(this.sequence ++ that.sequence)

  def ==(that: NGram[T]): Boolean = this.toString() == (that.toString())

  /** Returns all prefixes of n-gram. */
  def prefixes:List[NGram[T]] = {
    def aux(ngram : NGram[T]):List[NGram[T]] = ngram.order match {
      case 0 => Nil
      case _ => {
        ngram::aux(ngram.init)
      }
    }
    aux(this.init)
  }

  /** Returns all suffixes of n-gram. */
  def suffixes:List[NGram[T]] = {
    def aux(ngram : NGram[T]):List[NGram[T]] = ngram.order match {
      case 0 => Nil
      case _ => {
        ngram::aux(ngram.tail)
      }
    }
    aux(this.tail)
  }

  /** Returns n-gram prefix of given length. */
  def prefix(length:Int):NGram[T] =  NGram(sequence.take(length))

  /** Returns n-gram suffix of given length. */
  def suffix(length:Int):NGram[T] =  NGram(sequence.takeRight(length))


  /** String represetantion of the n-gram. */
  override def toString() = sequence.mkString(" ")

  /** Formats n-gram to KWIC representation. */
  def toKWIC[T](keyPhrase: List[T], n: Int = this.order): String = {
    val AvgWordLength = 6

    val idx = sequence.indexOfSlice(keyPhrase)

    // Split the sequence into 2 parts: before/after the key word
    val split = sequence.splitAt(idx)

    "%%%ds ".format(AvgWordLength * n).format(split._1.mkString(" ")) + split._2.mkString(" ")
  }


  /** Unigram composed of the first token of the n-gram. */
  def head: NGram[T] = NGram(Vector(sequence.head))

  /** New n-gram without the fist token. */
  def tail: NGram[T] = NGram(sequence.tail)

  /** New n-gram without the last token. */
  def init: NGram[T] = NGram(sequence.init)

  /** Unigram composed of the last token of the n-gram. */
  def last: NGram[T] = NGram(Vector(sequence.last))

  /** Order/length of an n-gram. */
  def order: Int = sequence.length

  /** Order/length of an n-gram. */
  def size: Int = sequence.length

  /** Order/length of an n-gram. */
  def n: Int = sequence.length

  /** Test if n-gram ends with a given token. */
  def endsWith(token: T): Boolean = endsWithSlice(List(token))

  /** Test if n-gram ends with a given sequence of tokens. */
  def endsWithSlice(slice: Seq[T]): Boolean = sequence.endsWith(slice)

  /** Test if n-gram starts with a given token. */
  def startsWith(token: T): Boolean = startsWithSlice(List(token))

  /** Test if n-gram starts with a given sequence of tokens. */
  def startsWithSlice(slice: Seq[T]): Boolean = sequence.startsWith(slice)

  /** Test if n-gram contains a given token. */
  def contains(token: T): Boolean = sequence.contains(token)

  /** Test if n-gram contains a given sequence of tokens. */
  def containsSlice(slice: Seq[T]): Boolean = sequence.containsSlice(slice)

  /** Test if n-gram contains a given token at a given position. */
  def containsAt(index: Int)(token: T): Boolean = sequence.indexOf(token, index - 1) == index - 1

  /** Test if n-gram contains a given sequence of tokens at a given position. */
  def containsSliceAt(index: Int)(slice: Seq[T]): Boolean = sequence.indexOfSlice(slice, index - 1) == index - 1

  /** Test if n-gram contains all given tokens from the sequence. */
  def containsAll(tokens: Seq[T]): Boolean = tokens.foldLeft(true)((b, token) => (b && contains(token)))

  /** Test if n-gram contains all given sequences of tokens from the sequence. */
  def containsAllSlices(slices: Seq[Seq[T]]): Boolean = slices.foldLeft(true)((b, slice) => (b && containsSlice(slice)))

  /** Test if n-gram contains any of given tokens from the sequence. */
  def containsAny(tokens: Seq[T]): Boolean = tokens.foldLeft(false)((b, token) => (b || contains(token)))

  /** Test if n-gram contains any of given sequences of tokens from the sequence. */
  def containsAnySlice(slices: Seq[Seq[T]]): Boolean = slices.foldLeft(false)((b, slice) => (b || containsSlice(slice)))

  /** Test if n-gram contains any of given tokens from the sequence at a given position. */
  def containsAnyAt(index: Int)(tokens: Seq[T]): Boolean = tokens.foldLeft(false)((b, token) => (b || containsAt(index)(token)))

  /** Test if n-gram contains any of given sequences of tokens from the sequence at a given position. */
  def containsAnySliceAt(index: Int)(slices: Seq[Seq[T]]): Boolean = slices.foldLeft(false)((b, slice) => (b || containsSliceAt(index)(slice)))


  // order function, first consider the sequence, then the order of ngram
  override def compare(that: NGram[T]): Int = this.toString() compareTo (that.toString())
}
