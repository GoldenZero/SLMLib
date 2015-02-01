package ch.uzh.cl.slmlib.tokenize

import org.apache.spark.Logging
import org.apache.spark.SparkContext._
/**
 * Tokenizer splits input strings into a sequence of tokens
 * Additionally, it is a good point to apply basic annotations on the input strings, e.g. POS, lemmatization
 * @tparam T Type of output tokens, e.g. String, (String,String) for POS-annotated tokens
 */
trait Tokenizer[T] extends Serializable with Logging {
  /** Splits lines into sequences of tokens. */
  def tokenize(lines: Iterator[String]): Iterator[Seq[T]]
}

object Tokenizer {
  /** Returns a basic, white-space split based, tokenizer */
  def defaultTokenizer = new {} with Tokenizer[String] {
    logInfo("Default Tokenizer initialized.")

    override def tokenize(lines: Iterator[String]): Iterator[Seq[String]] = lines.map(line => line.split("\\s+"))
  }
}