package ch.uzh.cl.slmlib.tokenize

import edu.stanford.nlp.tagger.maxent.MaxentTagger

/**
 * Wraps maximim entropy tagger <code>edu.stanford.nlp.tagger.maxent.MaxentTagger</code> to tokenize and POS-annotate input strings.
 */
class Tagger extends Tokenizer[(String, String)] {
  // instantiation will take place after shipping to the worker thread to avoid serialization of external class
  private lazy val tagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words/english-left3words-distsim.tagger")

  logInfo("POS-tagging Tokenizer initialized")

  /** Splits lines into tokens and returns sequences of tokens in (token,POS) form */
  override def tokenize(lines: Iterator[String]): Iterator[Seq[(String, String)]] = lines.map(line => tag(line))

  private def tag(line: String): Seq[(String, String)] = tagger.tagString(line).split("\\s").map(x => {
    val aux = x.split('_')
    (aux(0), aux(1))
  })
}


