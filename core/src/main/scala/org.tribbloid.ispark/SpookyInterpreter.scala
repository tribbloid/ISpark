package org.tribbloid.ispark

/**
 * Created by peng on 22/07/14.
 */
class SpookyInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SQLInterpreter(args, usejavacp) {

  override lazy val appName: String = "ISpooky"

  override def initializeSpark() {
    super.initializeSpark()

    interpret("""
import scala.concurrent.duration._
import org.tribbloid.spookystuff.actions._
import org.tribbloid.spookystuff.factory.driver._
import org.tribbloid.spookystuff.expressions._
import org.tribbloid.spookystuff.SpookyContext
              """) match {
      case Results.Failure(ee) => throw new RuntimeException("SpookyContext failed to be imported", ee)
      case Results.Success(value) =>
      case _ => throw new RuntimeException("SpookyContext failed to be imported")
    }
  }
}
