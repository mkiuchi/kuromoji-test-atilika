//
// kuromoji Japanese Tokenizer Sample
//   Atilika ver

import java.io.StringReader
import scala.collection.mutable.ArrayBuffer
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//import com.atilika.kuromoji
//import com.atilika.kuromoji.{TokenBase,TokenizerBase}
import com.atilika.kuromoji.ipadic.{Token,Tokenizer}
import scala.collection.JavaConverters._

object kuromojitest {
  def main(args: Array[String]) {

    //
    val conf = new SparkConf();
    conf.set("spark.app.name", "Kuromoji Test");
    val sc = new SparkContext(conf)

    val dictPath = "./dict/list.txt"

    def Mtokenize(sentence: String) :Seq[String] = {
      val word: ArrayBuffer[String] = new ArrayBuffer[String]()
      val ret = CustomTokenizer.tokenize(sentence, "")
      for (token <- ret) {
         word += token
      }
      word.toSeq
    }
    
    val input = sc.textFile("input.txt")
    val m = input.flatMap(x => Mtokenize(x))
    m.collect().foreach(x => println(x.toString))
  }
}

object CustomTokenizer {

  def tokenize(text: String, dictPath: String): List[String]  = {
    val retList:ArrayBuffer[String] = new ArrayBuffer[String]();
    val tk = new Tokenizer()
    val ar = tk.tokenize(text).asScala.toSet
    for(t <- ar) {
      val out = t.getSurface() + "\t" + t.getAllFeatures()
      retList += out
    }
    retList.toList
  }
}

