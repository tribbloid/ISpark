package org.tribbloid.ispark.display.dsl

import java.net.URL

import org.apache.spark.sql.DataFrame
import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl
import org.pegdown.{Extensions, PegDownProcessor}
import org.tribbloid.ispark.display.{HTMLDisplayObject, LatexDisplayObject}

import scala.xml._

object Display {

  case class Math(math: String) extends LatexDisplayObject {
    override val toLatex = "$$" + math + "$$"
  }

  case class Latex(latex: String) extends LatexDisplayObject {
    override val toLatex = latex
  }

  case class HTML(code: String) extends HTMLDisplayObject {
    override val toHTML: String = code
  }

  object MarkdownProcessor extends PegDownProcessor(Extensions.ALL)

  case class Markdown(code: String) extends HTMLDisplayObject {
    override val toHTML: String = {
      MarkdownProcessor.markdownToHtml(code)
    }
  }

  val TagSoupParser = new SAXFactoryImpl().newSAXParser()

  case class Table(
                    df: DataFrame,
                    limit:Int = 1000,
                    parse: Boolean = true,
                    random: Boolean = true
                    ) extends HTMLDisplayObject {
    assert(limit<=1000) //or parsing timeout

    override val toHTML: String = {

      val thead: Elem =
        <thead>
          <tr>{df.schema.fieldNames.map(str => <td>{str}</td>)}</tr>
        </thead>

      df.persist()
      val size = df.count()
      val rows = if (random) df.rdd.takeSample(withReplacement = false, num = limit)
      else df.rdd.take(limit)
      df.unpersist()

      val info: Elem =
        if (size < limit) <h5>returned {size} row(s) in total:</h5>
        else <h5>returned {size} rows in total but only {limit} of them are displayed:</h5>

      val body: Array[Elem] = rows.map{
        row =>
          val rowXml = row.toSeq.map{
            cell =>
              if (parse) {
                cell match {
                  case cell: NodeSeq => <td>{cell}</td>
                  case cell: NodeBuffer => <td>{cell}</td>
                  case cell: Any =>
                    try {
                      val cellXml = XML.loadXML(Source.fromString(cell.toString), TagSoupParser)
                      <td>{cellXml}</td>
                    }
                    catch {
                      case e: Throwable => <td>{cell}</td>
                    }
                  case _ => <td>{cell}</td>
                }
              }
              else {
                <td>{cell}</td>
              }
          }
          <tr>{rowXml}</tr>
      }

      val tbody: Elem =
        <tbody>
          {body}
        </tbody>

      val table: Elem =
        <table>
          {thead}
          {tbody}
        </table>

      (info ++ table).toString()
    }
  }

  class IFrame(src: URL, width: Int, height: Int) extends HTMLDisplayObject {
    override val toHTML: String =
      <iframe width={width.toString}
              height={height.toString}
              src={src.toString}
              frameborder="0"
              allowfullscreen="allowfullscreen"></iframe> toString()
  }

  object IFrame {
    def apply(src: URL, width: Int, height: Int): IFrame = new IFrame(src, width, height)

    def apply(src: String, width: Int, height: Int): IFrame = new IFrame(new URL(src), width, height)
  }

  case class YouTubeVideo(id: String, width: Int = 400, height: Int = 300)
    extends IFrame(new URL("https", "www.youtube.com", s"/embed/$id"), width, height)

  case class VimeoVideo(id: String, width: Int = 400, height: Int = 300)
    extends IFrame(new URL("https", "player.vimeo.com", s"/video/$id"), width, height)

  case class ScribdDocument(id: String, width: Int = 400, height: Int = 300)
    extends IFrame(new URL("https", "www.scribd.com", s"/embeds/$id/content"), width, height)

  case class ImageURL(url: URL, width: Option[Int], height: Option[Int]) extends HTMLDisplayObject {
    override val toHTML: String = <img src={url.toString}
                                       width={width.map(w => xml.Text(w.toString))}
                                       height={height.map(h => xml.Text(h.toString))}></img> toString()
  }

  object ImageURL {
    def apply(url: URL): ImageURL = ImageURL(url, None, None)

    def apply(url: String): ImageURL = ImageURL(new URL(url))

    def apply(url: URL, width: Int, height: Int): ImageURL = ImageURL(url, Some(width), Some(height))

    def apply(url: String, width: Int, height: Int): ImageURL = ImageURL(new URL(url), width, height)
  }

  //disabled because Json display is only supported in extension
  //  case class Json[T <: AnyRef](obj: T) extends JSONDisplayObject {
  //
  //    implicit val formats = DefaultFormats
  //    import org.json4s.jackson.Serialization
  //
  //    override val toJSON: String = {
  //      Serialization.write(obj) //TODO: Cannot serialize class created in interpreter
  //    }
  //  }
}