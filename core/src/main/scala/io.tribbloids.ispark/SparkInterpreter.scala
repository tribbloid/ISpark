package io.tribbloids.ispark

import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.repl.{SparkILoop, SparkCommandLine, SparkIMain}
import io.tribbloids.ispark.Util.debug

import scala.collection.mutable
import scala.tools.nsc.interpreter.IR
import scala.tools.nsc.io
import scala.tools.nsc.util.ClassPath
import scala.tools.nsc.util.Exceptional.unwrap

object Results {
  final case class Value(value: AnyRef, tpe: String)

  sealed trait Result
  final case class Success(value: Option[Value]) extends Result
  final case class Failure(exception: Throwable) extends Result
  final case object Error extends Result
  final case object Incomplete extends Result
  final case object Cancelled extends Result
}

class SparkInterpreter(args: Seq[String], usejavacp: Boolean=true) {

  val commandLine = {
    val cl = new SparkCommandLine(args.toList, println(_))
    cl.settings.embeddedDefaults[this.type]
    cl.settings.usejavacp.value = usejavacp
    val totalClassPath = SparkILoop.getAddedJars.foldLeft(
      cl.settings.classpath.value)((l, r) => ClassPath.join(l, r))
    cl.settings.classpath.value = totalClassPath
    cl
  }

  val output = new java.io.StringWriter
  val printer = new java.io.PrintWriter(output)

  var intp: SparkIMain = new SparkIMain(commandLine.settings, printer)
  var runner: Runner = new Runner(intp.classLoader)
  var session: Session = new Session
  var n: Int = 0

  var inMap: mutable.Map[Int, String] = mutable.Map()
  var outMap: mutable.Map[Int, Any] = mutable.Map()

  this.initializeSpark()

  def ++ = n += 1

  def initializeSpark() {
    val result = interpret(
      """
      import org.apache.spark.{SparkConf, SparkContext}
      val conf = new SparkConf().setAppName("iSpark")
      @transient val sc = new SparkContext(conf)

      import org.apache.spark.SparkContext._
      """)
  }

  def sparkCleanUp(){
    val result = interpret(
      """
      sc.stop()
      """)
  }

  def resetSpark() {
    synchronized {
      this.sparkCleanUp()

      this.initializeSpark()
    }
  }

  override def finalize() {
    try{
      synchronized {
        session.endSession(n)
      }
    }catch{
      case t: Throwable => throw t;
    }finally{
      super.finalize();
    }
  }

  def resetOutput() {
    output.getBuffer.setLength(0)
  }

  def completion = new IScalaCompletion(intp)

  def interpret(line: String, synthetic: Boolean = false): Results.Result = {
    // IMain#Request possibly != instance.Request
    // intp0 is needed as a stable identifier
    val intp0 = intp

    def requestFromLine(line: String, synthetic: Boolean): Either[IR.Result, intp0.Request] = {
      // Dirty hack to call a private method IMain.requestFromLine
      val method = classOf[SparkIMain].getDeclaredMethod("requestFromLine", classOf[String], classOf[Boolean])
      val args = Array(line, synthetic).map(_.asInstanceOf[AnyRef])
      method.setAccessible(true)
      method.invoke(intp0, args: _*).asInstanceOf[Either[IR.Result, intp0.Request]]
    }

    //TODO: why launching a new thread?
    // Unfortunately this has to be done otherwise throws classNotFoundError
    // Probably a classloader errror
    def loadAndRunReq(req: intp0.Request): Results.Result = {

      val definesValue = req.handlers.last.definesValue
      val name = if (definesValue) intp0.naming.sessionNames.result else intp0.naming.sessionNames.print

      val outcome =
        try {
          runner.execute {
            try {
              val value = req.lineRep.call(name)
              intp0.recordRequest(req)
              val outcome =
                if (definesValue && value != null) {
                  val tpe = intp0.typeOfTerm(intp0.mostRecentVar)
                  Some(Results.Value(value, intp0.global.afterTyper { tpe.toString }))
                } else
                  None
              Results.Success(outcome)
            } catch {
              case exception: Throwable =>
                req.lineRep.bindError(exception)
                Results.Failure(unwrap(exception))
            }
          }.result()
        } finally {
          runner.clear()
        }

      outcome
    }

    if (intp0.global == null) Results.Error
    else requestFromLine(line, synthetic) match {
      case Left(IR.Incomplete) => Results.Incomplete
      case Left(_) => Results.Error
      case Right(req)   =>
        // null indicates a disallowed statement type; otherwise compile
        // and fail if false (implying e.g. a type error)
        if (req == null || !req.compile) Results.Error
        else loadAndRunReq(req)
    }
  }

  def bind(name: String, boundType: String, value: Any, modifiers: List[String] = Nil): IR.Result = {
    val intp0 = intp

    val imports = (intp0.definedTypes ++ intp0.definedTerms) match {
      case Nil => "/* imports */"
      case names => names.map(intp0.pathToName).map("import " + _).mkString("\n  ")
    }

    val bindRep = new intp0.ReadEvalPrint()
    val source = s"""
            |object ${bindRep.evalName} {
            |  $imports
            |  var value: ${boundType} = _
            |  def set(x: Any) = value = x.asInstanceOf[${boundType}]
            |}
            """.stripMargin

    bindRep.compile(source)
    bindRep.callOpt("set", value).map { _ =>
      val line = "%sval %s = %s.value".format(modifiers map (_ + " ") mkString, name, bindRep.evalPath)
      intp0.interpret(line)
    } getOrElse IR.Error
  }

  def cancel() = runner.cancel()

  def stringify(obj: Any): String = intp.naming.unmangle(obj.toString)

  def typeInfo(code: String, deconstruct: Boolean): Option[String] = {
    val intp0 = intp
    import intp0.global._

    val symbol = intp0.symbolOfLine(code)
    if (symbol.exists) {
      Some(afterTyper {
        val info = symbol.info match {
          case NullaryMethodType(restpe) if symbol.isAccessor => restpe
          case info                                           => info
        }
        stringify(if (deconstruct) intp0.deconstruct.show(info) else info)
      })
    } else None
  }

  def createSparkContext(): SparkContext = {
    val execUri = System.getenv("SPARK_EXECUTOR_URI")
    val jars = SparkILoop.getAddedJars
    val conf = new SparkConf()
      .setMaster(getMaster())
      .setAppName("Spark shell")
      .setJars(jars)
      .set("spark.repl.class.uri", intp.classServer.uri)
    if (execUri != null) {
      conf.set("spark.executor.uri", execUri)
    }
    if (System.getenv("SPARK_HOME") != null) {
      conf.setSparkHome(System.getenv("SPARK_HOME"))
    }
    new SparkContext(conf)
  }

  private def getMaster(): String = {
    val master = {
        val envMaster = sys.env.get("MASTER")
        val propMaster = sys.props.get("spark.master")
        propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }
}

class Runner(classLoader: ClassLoader) {

  class Execution(body: => Results.Result) {
    private var _result: Option[Results.Result] = None

    private val lock     = new ReentrantLock()
    private val finished = lock.newCondition()

    private def withLock[T](body: => T) = {
      lock.lock()
      try body
      finally lock.unlock()
    }

    private def setResult(result: Results.Result) = withLock {
      _result = Some(result)
      finished.signal()
    }

    private val _thread = io.newThread {
      _.setContextClassLoader(classLoader)
    } {
      setResult(body)
    }

    private[Runner] def cancel() = if (running) setResult(Results.Cancelled)

    private[Runner] def interrupt() = _thread.interrupt()
    private[Runner] def stop()      = Threading.stop(_thread)

    def alive   = _thread.isAlive
    def running = !_result.isDefined

    def await()  = withLock { while (running) finished.await() }
    def result() = { await(); _result.getOrElse(sys.exit) }

    override def toString = s"Execution(thread=${_thread})"
  }

  private var current: Option[Execution] = None

  def execute(body: => Results.Result): Execution = {
    val execution = new Execution(body)
    current = Some(execution)
    execution
  }

  def clear() {
    current.foreach{ execution =>
      execution.cancel()
    }
    current = None
  }

  def cancel() {
    current.foreach { execution =>
      execution.interrupt()
      execution.cancel()
      io.timer(5) {
        if (execution.alive) {
          debug(s"Forcefully stopping ${execution}")
          execution.stop()
        }
      }
    }
  }
}