package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import scala.util.matching.Regex

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy", "Erlang")

  val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("wikipedia")
  val sc: SparkContext = new SparkContext(conf)
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.articles, 4)
  
  val langr: Map[String, Regex] = compileRegexes(langs)
  /**
   * compile a regular expression to match at word boundaries
   * \b = word boundary (when applicable)
   * (?i) = case insensitive
   * notes: \b is not alwas used, e.g "C#" and "C++" are not words,
   * 		therefore, their regular expression cannot end with \b because
   * 		they do not end with word characters.
   */
  def wordRegex(word: String) : Regex = {
    val startDelimiter = if ("+#".toList contains word.head) "" else "\\b"
    val endDelimiter = if ("+#".toList contains word.last) "" else "\\b"
    val escapedWord = word.replaceAll("[\\+\\*]", "\\\\$0")
    ("(?i)" + startDelimiter + escapedWord + endDelimiter).r
  }
  
  /**
   * precompile regexes and word->regex map to avoid repeatedly recompiling
   */
  def compileRegexes(langs: List[String]): Map[String, Regex] = {
    langs.map(lang => (lang, wordRegex(lang)))
    .foldLeft(Map[String, Regex]())((map, item) => map + (item._1 -> item._2))
  }
    
  def containsWord(article: WikipediaArticle, word: String, langr: Map[String, Regex]) : Boolean = {
    langr.get(word).get.findFirstIn(article.text) != None
  }
  
  def containsWord(article: WikipediaArticle, word: String) : Boolean = containsWord(article, word, langr)

  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.filter(containsWord(_, lang)).count().toInt
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val unordered = langs.foldLeft(Map[String, Int]())((count, lang) => count + (lang -> occurrencesOfLang(lang, rdd))).toList
    unordered.sortBy(pair => pair._2)(Ordering.Int.reverse)
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    sc.parallelize(langs.map(lang => (lang, rdd.filter(containsWord(_, lang)).collect.toList)))
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index.map(entry => (entry._1, entry._2.size)).collect.toList.sortBy(_._2)(Ordering.Int.reverse)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking is combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val index = rdd.flatMap(article => langs.filter(lang => containsWord(article, lang)).map(lang => (lang, List(article)))).reduceByKey((a, b) => (a union b).distinct)
    index.map(entry => (entry._1, entry._2.size)).reduceByKey(_ + _).collect.toList.sortBy(_._2)(Ordering.Int.reverse)
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))
	println(langsRanked)
	
    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))
	println(langsRanked2)
	
    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))
	println(langsRanked3)
	
    /* Output the speed of each ranking */
    println(timing)
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
