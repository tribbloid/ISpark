package org.tribbloid.ispooky

import org.apache.spark.sql.SQLContext
import org.tribbloid.ispark.interpreters.{Results, SparkInterpreter}

import scala.collection.immutable
import scala.language.reflectiveCalls
import scala.tools.nsc.interpreter.NamedParam

/**
 * Created by peng on 22/07/14.
 */
class SpookyInterpreter(
                         output: java.io.StringWriter = new java.io.StringWriter,
                         master: Option[String] = None,
                         usejavacp: Boolean=true,
                         appName: String = "ISpooky"
                         )
  extends SparkInterpreter(output, master, usejavacp, appName) {

//  @DeveloperApi
//  def createSpookyContext(): SpookyContext = {
//    val name = "org.tribbloid.spookystuff.SpookyContext"
//    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(this.getClass.getClassLoader)
//    val spooky = loader.loadClass(name).getConstructor(classOf[SQLContext])
//      .newInstance(sqlContext).asInstanceOf[SpookyContext]
//
//    logInfo("Created SpookyContext..")
//
//    spooky
//  }

  override def importSpark() {

    super.importSpark()

    import scala.tools.nsc.interpreter.{Results => IR}

    quietBind(NamedParam[SQLContext]("sqlContext", this.createSQLContext()), immutable.List("@transient")) match {
      case IR.Success =>
      case _ => throw new RuntimeException("SQLContext failed to initialize\n"+this.output.toString)
    }

//    quietBind(NamedParam[SpookyContext]("spooky", this.createSpookyContext()), immutable.List("@transient")) match {
//      case IR.Success =>
//      case _ => throw new RuntimeException("SpookyContext failed to initialize\n"+this.output.toString)
//    }

    interpretGetResult(
      """
        |@transient val spooky = new org.tribbloid.spookystuff.SpookyContext(sqlContext)
      """.stripMargin) match {
      case _: Results.Success =>
      case Results.Exception(ee) => throw new RuntimeException("SpookyContext failed to be imported", ee)
      case _ => throw new RuntimeException("SpookyContext failed to be imported\n"+this.output.toString)
    }

    interpretGetResult("""
                         |import scala.concurrent.duration._
                         |import org.tribbloid.spookystuff.actions._
                         |import org.tribbloid.spookystuff.dsl._
                         |import spooky._
                       """.stripMargin) match {
      case _: Results.Success =>
      case Results.Exception(ee) => throw new RuntimeException("SpookyContext failed to be imported", ee)
      case _ => throw new RuntimeException("SpookyContext failed to be imported\n"+this.output.toString)
    }
  }
}
