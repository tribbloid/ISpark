// scalastyle:off

/* NSC -- new Scala compiler
 * Copyright 2005-2013 LAMP/EPFL
 * @author Alexander Spoon
 */

package org.apache.spark.repl

import java.io.BufferedReader

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.util.Utils

import scala.Predef.{println => _, _}
import scala.collection.immutable
import scala.language.{existentials, implicitConversions, postfixOps}
import scala.reflect._
import scala.reflect.api.{Mirror, TypeCreator, Universe => ApiUniverse}
import scala.tools.nsc.interpreter._
import scala.tools.nsc.util.ScalaClassLoader._
import scala.tools.nsc.{io, _}

/** The Scala interactive shell.  It provides a read-eval-print loop
  *  around the Interpreter class.
  *  After instantiation, clients should call the main() method.
  *
  *  If no in0 is specified, then input will come from the console, and
  *  the class will attempt to provide input editing feature such as
  *  input history.
  *
  *  @author Moez A. Abdel-Gawad
  *  @author  Lex Spoon
  *  @version 1.2
  */
@DeveloperApi
class SparkILoopExt(
                     in0: Option[BufferedReader],
                     out: JPrintWriter,
                     master: Option[String]
                     ) extends SparkILoop(in0, out, master) {
  def this(in0: BufferedReader, out: JPrintWriter, master: String) = this(Some(in0), out, Some(master))
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out, None)
  def this() = this(None, new JPrintWriter(Console.out, true), None)

  def getSettings = settings
  def getIntp = intp

  def sparkCleanUp(){
    echo("Stopping spark context.")
    intp.beQuietDuring {
      intp.interpret("sc.stop()")
    }
  }
  /** Close the interpreter and set the var to null. */
  def closeInterpreter() {
    if (intp ne null) {
      sparkCleanUp()
      intp.close()
      intp = null
    }
  }

  //  override def echo(msg: String) = {
  //    out println msg
  //    out.flush()
  //  }
  private def echoNoNL(msg: String) = {
    out print msg
    out.flush()
  }

  /** Tries to create a JLineReader, falling back to SimpleReader:
    *  unless settings or properties are such that it should start
    *  with SimpleReader.
    */
  private def chooseReader(settings: Settings): InteractiveReader = {
    if (settings.Xnojline.value || Properties.isEmacsShell)
      SimpleReader()
    else try new SparkJLineReader(
      if (settings.noCompletion.value) NoCompletion
      else new SparkJLineCompletion(intp)
    )
    catch {
      case ex @ (_: Exception | _: NoClassDefFoundError) =>
        echo("Failed to created SparkJLineReader: " + ex + "\nFalling back to SimpleReader.")
        SimpleReader()
    }
  }

  private val u: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe
  private val m = u.runtimeMirror(Utils.getSparkClassLoader)
  private def tagOfStaticClass[T: ClassTag]: u.TypeTag[T] =
    u.TypeTag[T](
      m,
      new TypeCreator {
        def apply[U <: ApiUniverse with Singleton](m: Mirror[U]): U # Type =
          m.staticClass(classTag[T].runtimeClass.getName).toTypeConstructor.asInstanceOf[U # Type]
      })

  def process(settings: Settings): Boolean = savingContextLoader {
    if (getMaster == "yarn-client") System.setProperty("SPARK_YARN_MODE", "true")

    this.settings = settings
    createInterpreter()

    val autorun = replProps.replAutorunCode.option flatMap (f => io.File(f).safeSlurp())
    if (autorun.isDefined) intp.quietRun(autorun.get)

    this.intp.initializeSynchronous()

    lazy val tagOfSparkIMain = tagOfStaticClass[org.apache.spark.repl.SparkIMain]
    intp.quietBind(NamedParam[SparkIMain]("$intp", intp)(tagOfSparkIMain, classTag[SparkIMain]))

    importSpark()

    if (intp.reporter.hasErrors) return false
    else true
  }

  def bind(p: NamedParam, modifiers: List[String]): IR.Result = this.bind(p.name, p.tpe, p.value, modifiers)

  def quietBind(p: NamedParam, modifiers: List[String]): IR.Result = this.beQuietDuring(bind(p, modifiers))

  protected def importSpark() {
    quietBind(NamedParam[SparkContext]("sc", this.createSparkContext()), immutable.List("@transient")) match {
      case IR.Success =>
      case _ => throw new RuntimeException("SparkContext failed to initialize")
    }

    val result = this.beQuietDuring{
      this.interpret("""
                   |import org.apache.spark.SparkContext._
                   |import org.tribbloid.ispark.display.dsl._
                 """.stripMargin)
    }
    result match {
      case IR.Success =>
      case _ => throw new RuntimeException("SparkContext failed to be imported")
    }
  }

  /** process command-line arguments and do as they request */
  override def process(args: Array[String]): Boolean = {
    val command = new SparkCommandLine(args.toList, msg => echo(msg))
    def neededHelp(): String =
      (if (command.settings.help.value) command.usageMsg + "\n" else "") +
        (if (command.settings.Xhelp.value) command.xusageMsg + "\n" else "")

    // if they asked for no help and command is valid, we call the real main
    neededHelp() match {
      case "" => command.ok && process(command.settings)
      case help => echoNoNL(help); true
    }
  }

  private def getMaster: String = {
    val master = this.master match {
      case Some(m) => m
      case None =>
        val envMaster = sys.env.get("MASTER")
        val propMaster = sys.props.get("spark.master")
        propMaster.orElse(envMaster).getOrElse("local[*]")
    }
    master
  }
}