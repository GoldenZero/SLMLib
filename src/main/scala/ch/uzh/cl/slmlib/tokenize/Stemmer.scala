package ch.uzh.cl.slmlib.tokenize

import org.tartarus.snowball.ext.EnglishStemmer

/**
 * White-space split based, tokenizer with tokens stemmed by <code>org.tartarus.snowball.ext.EnglishStemmer</code> (distributed with Lucene).
 */
class Stemmer extends Tokenizer[String] {
  // instantiation will take place after shipping to the worker thread to avoid serialization of external class
  private lazy val stemmer = new EnglishStemmer

  logInfo("Stemming Tokenizer initialized")

  /** Splits lines into tokens and returns sequences of tokens in stemmed form */
  def tokenize(lines: Iterator[String]): Iterator[Seq[String]] = lines.map(line => line.split("\\s+").toList.map(term => stem(term)))

  private def stem(token: String): String = {
    stemmer.setCurrent(token)
    stemmer.stem()
    stemmer.getCurrent
  }


}


