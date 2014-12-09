package org.tribbloid.ispark

import org.apache.spark.sql.SQLContext

import scala.collection.immutable
import scala.tools.nsc.interpreter._

/**
 * Created by peng on 22/07/14.
 */
class SQLInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SparkInterpreter(args, usejavacp) {

  override def initializeSpark() {
    super.initializeSpark()

    val sqlContext = new SQLContext(this.sc)
    intp.quietBind(NamedParam[SQLContext]("sqlContext", sqlContext), immutable.List("@transient")) match {
      case IR.Success =>
      case _ => throw new RuntimeException("SQLContext failed to initialize")
    }

    //TODO: this part doesn't work
//    interpret("""
//import sqlContext._
//              """) match {
//      case Results.Success(value) =>
//      case Results.Failure(ee) => throw new RuntimeException("SQLContext failed to be imported", ee)
//      case _ => throw new RuntimeException("SQLContext failed to be imported")
//    }
  }
}