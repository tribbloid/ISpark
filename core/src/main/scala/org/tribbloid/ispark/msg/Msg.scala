package org.tribbloid.ispark.msg

import org.tribbloid.ispark.display.{Data, MIME}
import org.tribbloid.ispark.{UUID, Util}

object ExecutionStatuses extends Enumeration {
  type ExecutionStatus = Value
  val ok = Value
  val error = Value
  val abort = Value
}

object HistAccessTypes extends Enumeration {
  type HistAccessType = Value
  val range = Value
  val tail = Value
  val search = Value
}

object ExecutionStates extends Enumeration {
  type ExecutionState = Value
  val busy = Value
  val idle = Value
  val starting = Value
}

object MsgTypes extends Enumeration {
  type MsgType = Value

  val execute_request,
  execute_reply,
  inspect_request,
  inspect_reply,
  complete_request,
  complete_reply,
  history_request,
  history_reply,
  connect_request,
  connect_reply,
  kernel_info_request,
  kernel_info_reply,
  shutdown_request,
  shutdown_reply,
  stream,
  display_data,
  execute_input,
  execute_result,
  error,
  status,
  input_request,
  input_reply,
  comm_open,
  comm_msg,
  comm_close = Value
}

sealed trait Content
sealed trait FromIPython extends Content
sealed trait ToIPython extends Content

case class Header(
                   msg_id: UUID,
                   username: String,
                   session: UUID,
                   msg_type: MsgType,
                   version: String = "5.0"
                   )

case class Msg[+T <: Content](
                               idents: List[String], // XXX: Should be List[UUID]?
                               header: Header,
                               parent_header: Option[Header],
                               metadata: Metadata,
                               content: T) {

  private def replyHeader(msg_type: MsgType): Header =
    header.copy(msg_id=UUID.uuid4(), msg_type=msg_type)

  private def replyMsg[V <: ToIPython](idents: List[String], msg_type: MsgType, content: V, metadata: Metadata): Msg[V] =
    Msg(idents, replyHeader(msg_type), Some(header), metadata, content)

  def pub[V <: ToIPython](msg_type: MsgType, content: V, metadata: Metadata=Metadata()): Msg[V] = {
    val tpe = content match {
      case content: stream => content.name
      case _               => msg_type.toString
    }
    replyMsg(tpe :: Nil, msg_type, content, metadata)
  }

  def reply[V <: ToIPython](msg_type: MsgType, content: V, metadata: Metadata=Metadata()): Msg[V] =
    replyMsg(idents, msg_type, content, metadata)
}

case class execute_request(
                            // Source code to be executed by the kernel, one or more lines.
                            code: String,

                            // A boolean flag which, if True, signals the kernel to execute
                            // this code as quietly as possible.  This means that the kernel
                            // will compile the code with 'exec' instead of 'single' (so
                            // sys.displayhook will not fire), forces store_history to be False,
                            // and will *not*:
                            //   - broadcast exceptions on the PUB socket
                            //   - do any logging
                            //
                            // The default is False.
                            silent: Boolean,

                            // A boolean flag which, if True, signals the kernel to populate history
                            // The default is True if silent is False.  If silent is True, store_history
                            // is forced to be False.
                            store_history: Option[Boolean]=None,

                            // A dict mapping names to expressions to be evaluated in the user's dict. The
                            // rich display-data representation of each will be evaluated after execution.
                            // See the display_data content for the structure of the representation data.
                            user_expressions: Map[String, String],

                            // Some frontends (e.g. the Notebook) do not support stdin requests. If
                            // raw_input is called from code executed from such a frontend, a
                            // StdinNotImplementedError will be raised.
                            allow_stdin: Boolean) extends FromIPython

sealed trait execute_reply extends ToIPython {
  // One of: 'ok' OR 'error' OR 'abort'
  val status: ExecutionStatus

  // The global kernel counter that increases by one with each request that
  // stores history.  This will typically be used by clients to display
  // prompt numbers to the user.  If the request did not store history, this will
  // be the current value of the counter in the kernel.
  val execution_count: Int
}

case class execute_ok_reply(
                             execution_count: Int,

                             // 'payload' will be a list of payload dicts.
                             // Each execution payload is a dict with string keys that may have been
                             // produced by the code being executed.  It is retrieved by the kernel at
                             // the end of the execution and sent back to the front end, which can take
                             // action on it as needed.  See main text for further details.
                             payload: List[Map[String, String]],

                             // Results for the user_expressions.
                             user_expressions: Map[String, String]) extends execute_reply {

  val status = ExecutionStatuses.ok
}

case class execute_error_reply(
                                execution_count: Int,

                                // Exception name, as a string
                                ename: String,
                                // Exception value, as a string
                                evalue: String,

                                // The traceback will contain a list of frames, represented each as a
                                // string.  For now we'll stick to the existing design of ultraTB, which
                                // controls exception level of detail statefully.  But eventually we'll
                                // want to grow into a model where more information is collected and
                                // packed into the traceback object, with clients deciding how little or
                                // how much of it to unpack.  But for now, let's start with a simple list
                                // of strings, since that requires only minimal changes to ultratb as
                                // written.
                                traceback: List[String]) extends execute_reply {

  val status = ExecutionStatuses.error
}

case class execute_abort_reply(
                                execution_count: Int) extends execute_reply {

  val status = ExecutionStatuses.abort
}

case class inspect_request(
                            //# The code context in which introspection is requested
                            //# this may be up to an entire multiline cell.
                            code: String,

                            //# The cursor position within 'code' (in unicode characters) where inspection is requested
                            cursor_pos: Int,

                            //# The level of detail desired.  In IPython, the default (0) is equivalent to typing
                            //# 'x?' at the prompt, 1 is equivalent to 'x??'.
                            //# The difference is up to kernels, but in IPython level 1 includes the source code
                            //# if available.
                            detail_level: Int
                            ) extends FromIPython

case class inspect_reply(
                          //# 'ok' if the request succeeded or 'error', with error information as in all other replies.
                          status: ExecutionStatus,

                          //# data can be empty if nothing is found
                          data: Data,
                          metadata: Metadata
                          ) extends ToIPython

case class complete_request(
                             // The code context in which completion is requested
                             // this may be up to an entire multiline cell, such as
                             // 'foo = a.isal'
                             code: String,

                             // The position of the cursor where the user hit 'TAB' on the line.
                             cursor_pos: Int) extends FromIPython

case class complete_reply(
                           // The list of all matches to the completion request, such as
                           // ['a.isalnum', 'a.isalpha'] for the above example.
                           matches: List[String],

                           // the substring of the matched text
                           // this is typically the common prefix of the matches,
                           // and the text that is already in the block that would be replaced by the full completion.
                           // This would be 'a.is' in the above example.
                           matched_text: String,

                           // status should be 'ok' unless an exception was raised during the request,
                           // in which case it should be 'error', along with the usual error message content
                           // in other messages.
                           status: ExecutionStatus) extends ToIPython

case class history_request(
                            // If True, also return output history in the resulting dict.
                            output: Boolean,

                            // If True, return the raw input history, else the transformed input.
                            raw: Boolean,

                            // So far, this can be 'range', 'tail' or 'search'.
                            hist_access_type: HistAccessType,

                            // If hist_access_type is 'range', get a range of input cells. session can
                            // be a positive session number, or a negative number to count back from
                            // the current session.
                            session: Option[Int],

                            // start and stop are line numbers within that session.
                            start: Option[Int],
                            stop: Option[Int],

                            // If hist_access_type is 'tail' or 'search', get the last n cells.
                            n: Option[Int],

                            // If hist_access_type is 'search', get cells matching the specified glob
                            // pattern (with * and ? as wildcards).
                            pattern: Option[String],

                            // If hist_access_type is 'search' and unique is true, do not
                            // include duplicated history.  Default is false.
                            unique: Option[Boolean]) extends FromIPython

case class history_reply(
                          // A list of 3 tuples, either:
                          // (session, line_number, input) or
                          // (session, line_number, (input, output)),
                          // depending on whether output was False or True, respectively.
                          history: List[(Int, Int, Either[String, (String, Option[String])])]) extends ToIPython

case class connect_request() extends FromIPython

case class connect_reply(
                          // The port the shell ROUTER socket is listening on.
                          shell_port: Int,
                          // The port the PUB socket is listening on.
                          iopub_port: Int,
                          // The port the stdin ROUTER socket is listening on.
                          stdin_port: Int,
                          // The port the heartbeat socket is listening on.
                          hb_port: Int) extends ToIPython

case class kernel_info_request() extends FromIPython

case class kernel_info_reply(
                              // Version of messaging protocol (mandatory).
                              // The first integer indicates major version.  It is incremented when
                              // there is any backward incompatible change.
                              // The second integer indicates minor version.  It is incremented when
                              // there is any backward compatible change.
                              protocol_version: String = "5.0",

                              //# The kernel implementation name
                              //# (e.g. 'ipython' for the IPython kernel)
                              implementation: String = "iSpark",

                              //# Implementation version number.
                              //# The version number of the kernel's implementation
                              //# (e.g. IPython.__version__ for the IPython kernel)
                              implementation_version: String = "0.2.0",

                              //# Information about the language of code for the kernel
                              language_info: LanguageInfo = LanguageInfo(),

                              //# A banner of information about the kernel,
                              //# which may be desplayed in console environments.
                              banner: String = Util.kernel_info

                              //# Optional: A list of dictionaries, each with keys 'text' and 'url'.
                              //# These will be displayed in the help menu in the notebook UI.
                              //'help_links': [
                              //{'text': str, 'url': str}
                              //],

                              ) extends ToIPython

case class LanguageInfo(
                         //# Name of the programming language in which kernel is implemented.
                         //# Kernel included in IPython returns 'python'.
                         name: String = "Scala",

                         //# Language version number.
                         //# It is Python version number (e.g., '2.7.3') for the kernel
                         //# included in IPython.
                         version: String = Util.scalaVersion,

                         //# mimetype for script files in this language
                         mimetype: String = "text/x-scala",

                         //# Extension without the dot, e.g. 'py'
                         file_extension: String = "scala",

                         //# Pygments lexer, for highlighting
                         //# Only needed if it differs from the top level 'language' field.
                         //                          pygments_lexer: String,

                         //# Codemirror mode, for for highlighting in the notebook.
                         //# Only needed if it differs from the top level 'language' field.
                         codemirror_mode: String = "Scala"

                         //# Nbconvert exporter, if notebooks written with this kernel should
                         //# be exported with something other than the general 'script'
                         //# exporter.
                         //                          nbconvert_exporter: String
                         ) extends ToIPython

case class shutdown_request(
                             // whether the shutdown is final, or precedes a restart
                             restart: Boolean) extends FromIPython

case class shutdown_reply(
                           // whether the shutdown is final, or precedes a restart
                           restart: Boolean) extends ToIPython

case class stream(
                   // The name of the stream is one of 'stdout', 'stderr'
                   name: String,

                   // The data is an arbitrary string to be written to that stream
                   text: String) extends ToIPython

//TODO: Changed in version 5.0: application/json data should be unpacked JSON data, not double-serialized as a JSON string.
//See http://ipython.org/ipython-doc/3/development/messaging.html#messaging
case class display_data(
                         // Who create the data
                         source: String,

                         // The data dict contains key/value pairs, where the kids are MIME
                         // types and the values are the raw data of the representation in that
                         // format.
                         data: Data,

                         // Any metadata that describes the data
                         metadata: Metadata
                         ) extends ToIPython

case class execute_input(
                          // Source code to be executed, one or more lines
                          code: String,

                          // The counter for this execution is also provided so that clients can
                          // display it, since IPython automatically creates variables called _iN
                          // (for input prompt In[N]).
                          execution_count: Int) extends ToIPython

case class execute_result(
                           // The counter for this execution is also provided so that clients can
                           // display it, since IPython automatically creates variables called _N
                           // (for prompt N).
                           execution_count: Int,

                           // data and metadata are identical to a display_data message.
                           // the object being displayed is that passed to the display hook,
                           // i.e. the *result* of the execution.
                           data: Data,
                           metadata: Metadata = Metadata()
                           ) extends ToIPython

case class error(
                  execution_count: Int,

                  // Exception name, as a string
                  ename: String,
                  // Exception value, as a string
                  evalue: String,

                  // The traceback will contain a list of frames, represented each as a
                  // string.  For now we'll stick to the existing design of ultraTB, which
                  // controls exception level of detail statefully.  But eventually we'll
                  // want to grow into a model where more information is collected and
                  // packed into the traceback object, with clients deciding how little or
                  // how much of it to unpack.  But for now, let's start with a simple list
                  // of strings, since that requires only minimal changes to ultratb as
                  // written.
                  traceback: List[String]) extends ToIPython

object error {
  // XXX: can't use apply(), because of https://github.com/playframework/playframework/issues/2031
  def fromThrowable(execution_count: Int, exception: Throwable): error = {
    val name = exception.getClass.getName
    val value = Option(exception.getMessage) getOrElse ""
    val stacktrace = exception
      .getStackTrace
      .takeWhile(_.getFileName != "<console>")
      .toList
    val traceback = s"$name: $value" :: stacktrace.map("    " + _)

    error(execution_count=execution_count,
      ename=name,
      evalue=value,
      traceback=traceback)
  }
}

//TODO: Changed in version 5.0: Busy and idle messages should be sent before/after handling every message, not just execution.
case class status(
                   // When the kernel starts to execute code, it will enter the 'busy'
                   // state and when it finishes, it will enter the 'idle' state.
                   // The kernel will publish state 'starting' exactly once at process startup.
                   execution_state: ExecutionState) extends ToIPython

case class clear_output(
                         // Wait to clear the output until new output is available.  Clears the
                         // existing output immediately before the new output is displayed.
                         // Useful for creating simple animations with minimal flickering.
                         _wait: Boolean) extends ToIPython

case class input_request(
                          prompt: String,
                          password: Boolean = false
                          ) extends ToIPython

case class input_reply(
                        value: String) extends FromIPython

import play.api.libs.json.JsObject

case class comm_open(
                      comm_id: UUID,
                      target_name: String,
                      data: JsObject) extends ToIPython with FromIPython

case class comm_msg(
                     comm_id: UUID,
                     data: JsObject) extends ToIPython with FromIPython

case class comm_close(
                       comm_id: UUID,
                       data: JsObject) extends ToIPython with FromIPython

// XXX: This was originally in src/main/scala/Formats.scala, but due to
// a bug in the compiler related to `knownDirectSubclasses` and possibly
// also other bugs (e.g. `isCaseClass`), formats had to be moved here
// and explicit type annotations had to be added for formats of sealed
// traits. Otherwise no known subclasses will be reported.

import org.tribbloid.ispark.json.{EnumJson, Json}
import play.api.libs.json.{JsObject, Writes}

package object formats {

  implicit val MIMEFormat = new Writes[MIME] {
    def writes(mime: MIME) = implicitly[Writes[String]].writes(mime.name)
  }

  implicit val DataFormat = new Writes[Data] {
    def writes(data: Data) = {
      JsObject(data.items.map { case (mime, value) =>
        mime.name -> implicitly[Writes[String]].writes(value)
      })
    }
  }

  import org.tribbloid.ispark.json.JsonImplicits._ //DO NOT omit this line!

  implicit val MsgTypeFormat = EnumJson.format(MsgTypes)
  implicit val HeaderFormat = Json.format[Header]

  implicit val ExecutionStatusFormat = EnumJson.format(ExecutionStatuses)
  implicit val ExecutionStateFormat = EnumJson.format(ExecutionStates)
  implicit val HistAccessTypeFormat = EnumJson.format(HistAccessTypes)

  implicit val ExecuteRequestJSON = Json.format[execute_request]
  implicit val ExecuteReplyJSON: Writes[execute_reply] = Json.writes[execute_reply]

  implicit val ObjectInfoRequestJSON = Json.format[inspect_request]
  implicit val ObjectInfoReplyJSON: Writes[inspect_reply] = Json.writes[inspect_reply]

  implicit val CompleteRequestJSON = Json.format[complete_request]
  implicit val CompleteReplyJSON = Json.format[complete_reply]

  implicit val HistoryRequestJSON = Json.format[history_request]
  implicit val HistoryReplyJSON = Json.format[history_reply]

  implicit val ConnectRequestJSON = Json.noFields[connect_request]
  implicit val ConnectReplyJSON = Json.format[connect_reply]

  implicit val KernelInfoRequestJSON = Json.noFields[kernel_info_request]
  implicit val LanguageInfoJSON = Json.format[LanguageInfo]
  implicit val KernelInfoReplyJSON = Json.format[kernel_info_reply]

  implicit val ShutdownRequestJSON = Json.format[shutdown_request]
  implicit val ShutdownReplyJSON = Json.format[shutdown_reply]

  implicit val StreamJSON = Json.writes[stream]
  implicit val DisplayDataJSON = Json.writes[display_data]
  implicit val PyinJSON = Json.writes[execute_input]
  implicit val PyoutJSON = Json.writes[execute_result]
  implicit val PyerrJSON = Json.writes[error]
  implicit val StatusJSON = Json.writes[status]
  implicit val ClearOutputJSON = new Writes[clear_output] {
    def writes(obj: clear_output) = {
      // NOTE: `wait` is a final member on Object, so we have to go through hoops
      JsObject(Seq("wait" -> implicitly[Writes[Boolean]].writes(obj._wait)))
    }
  }

  implicit val InputRequestJSON = Json.format[input_request]
  implicit val InputReplyJSON = Json.format[input_reply]

  implicit val CommOpenJSON = Json.format[comm_open]
  implicit val CommMsgJSON = Json.format[comm_msg]
  implicit val CommCloseJSON = Json.format[comm_close]
}
