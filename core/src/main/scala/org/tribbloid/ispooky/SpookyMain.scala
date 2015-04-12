package org.tribbloid.ispooky

import org.tribbloid.ispark.{Main, Options, Util}

/**
 * Created by peng on 12/9/14.
 */
object SpookyMain {

  def main (args: Array[String]) {
    Util.options = new Options(args)
    Util.daemon = new SpookyMain(Util.options)
    Util.daemon.heartBeat.join()
  }
}

class SpookyMain(options: Options) extends Main(options) {

  override lazy val interpreter = new SpookyInterpreter()
}