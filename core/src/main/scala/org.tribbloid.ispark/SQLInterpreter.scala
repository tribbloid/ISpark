package org.tribbloid.ispark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.collection.immutable
import scala.tools.nsc.interpreter._

/**
 * Created by peng on 22/07/14.
 */
class SQLInterpreter(args: Seq[String], usejavacp: Boolean=true)
  extends SparkInterpreter(args, usejavacp) {

  override lazy val appName: String = "ISpooky"

  var sql: SQLContext = _

  override def initializeSpark() {
    super.initializeSpark()

    sql = new SQLContext(this.sc)
    intp.quietBind(NamedParam[SQLContext]("sql", sql), immutable.List("@transient")) match {
      case IR.Success => return
      case _ => throw new RuntimeException("SQL failed to initialize")
    }
  }

  override def sparkCleanUp(): Unit = {
    super.sparkCleanUp()

    if (sql!=null) {
      sql = null
    }
  }
}
