package org.tribbloid.ispark

/**
 * Created by peng on 22/07/14.
 */
class SpookyInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SparkInterpreter(args, usejavacp) {

  override val appName = "ISpooky"

  override def initializeSpark() {
    super.initializeSpark()

    interpret("""
      import org.tribbloid.spookystuff.SpookyContext._
      import org.tribbloid.spookystuff.entity._
              """) match {
      case Results.Failure(ee) => throw new RuntimeException("SparkContext failed to be imported", ee)
      case Results.Success(value) => return
      case _ => throw new RuntimeException("SparkContext failed to be imported")
    }
  }
}
