package org.tribbloid.ispark.interpreters

import org.apache.spark.repl.{SparkCommandLine, SparkILoopExt, SparkJLineCompletion}
import org.tribbloid.ispark.Util
import org.tribbloid.ispark.display.Data

import scala.language.postfixOps
import scala.tools.nsc.interpreter._

class SparkInterpreter(
                        val output: java.io.StringWriter = new java.io.StringWriter,
                        master: Option[String] = None,
                        usejavacp: Boolean=true,
                        val appName: String = "ISpark"
                        ) extends SparkILoopExt(None, new java.io.PrintWriter(output), master) { //no input stream except interpret

  def init(args: Array[String]): Unit = assert(this.process(args))

  override def process(args: Array[String]): Boolean = {
    val command = new SparkCommandLine(args.toList, println(_))
    assert(command.ok)

    command.settings.embeddedDefaults(this.getClass.getClassLoader) //this ClassLoader link is optional if no bind is used.

    command.settings.usejavacp.value = usejavacp

    command.ok && process(command.settings)
  }

  override def finalize() {
    try{
      closeAll()
    }catch{
      case e: Throwable => Util.log("FINALIZATION FAILED! " + e);
    }finally{
      super.finalize()
    }
  }

  def resetOutput() {
    output.getBuffer.setLength(0)
  }

  private lazy val completion = new SparkJLineCompletion(this)

  def completions(input: String): List[String] = { //TODO: need more testing and comparison with old implementation
    val c: Completion.ScalaCompleter = completion.completer()
    val ret: Completion.Candidates = c.complete(input, input.length)
    ret.candidates
  }

  //  def interpretEnsureSuccess(code: String): Unit = {
  //    val result = intp.interpret(code)
  //    assert(result == IR.Success,
  //      s"""
  //         |Fail to interpret:
  //         |$code
  //          |$result
  //          |${this.out}
  //       """.stripMargin
  //    )
  //  }

  def interpretGetResult(line: String): Results.Result = {

    val res = try{
      this.interpretBlock(line)
    }
    catch {
      case e: Throwable => return Results.Exception(e)
    }

    res match {
      case IR.Incomplete => Results.Incomplete
      case IR.Error =>
        Results.Error
      case IR.Success =>
        val mostRecentVar = this.mostRecentVar
        if (mostRecentVar == "") Results.NoValue
        else {
          val value = this.valueOfTerm(mostRecentVar)
          value match {
            case None => Results.NoValue
            case Some(v) =>
              val tpe = this.typeOfTerm(mostRecentVar)

              val data = Data.parse(v)
              val result = Results.Value(v, tpe.toString(), data)
              result
          }
        }
    }
  }

  def cancel() = {
    if (this.sparkContext != null) {
      this.sparkContext.cancelAllJobs()
    }
  }

  def typeInfo(code: String, verbose: Boolean): Option[String] = {

    val symbol = this.symbolOfLine(code)

    val tpe = this.typeOfExpression("code", silent = true)
    val info = tpe.toString()

    Some(info)
  }
}

