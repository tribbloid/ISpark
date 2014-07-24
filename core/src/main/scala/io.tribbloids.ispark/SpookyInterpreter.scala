package io.tribbloids.ispark

/**
 * Created by peng on 22/07/14.
 */
class SpookyInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SparkInterpreter(args, usejavacp) {

  override def initializeSpark() {
    val result = interpret(
      """
      import org.apache.spark.{SparkConf, SparkContext}
      val conf = new SparkConf().setAppName("iSpark")
      @transient val sc = new SparkContext(conf)

      import org.apache.spark.SparkContext._
      import org.tribbloid.spookystuff.SpookyContext._
      import org.tribbloid.spookystuff.entity._
      """)
  }
}
