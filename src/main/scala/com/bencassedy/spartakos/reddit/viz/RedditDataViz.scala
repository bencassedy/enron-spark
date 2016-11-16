package com.bencassedy.spartakos.reddit.viz


import io.continuum.bokeh._
import math.{Pi => pi, sin}

object RedditDataViz extends App with Tools {
  object source extends ColumnDataSource {
    val x = column(-2*pi to 2*pi by 0.1)
    val y = column(x.value.map(sin))
  }

  import source.{x,y}

  val xdr = new DataRange1d()
  val ydr = new DataRange1d()

  val plot = new Plot().x_range(xdr).y_range(ydr).tools(Pan|WheelZoom)

  val xaxis = new LinearAxis().plot(plot).location(Location.Below)
  val yaxis = new LinearAxis().plot(plot).location(Location.Left)
  plot.below <<= (xaxis :: _)
  plot.left <<= (yaxis :: _)

  val glyph = new Circle().x(x).y(y).size(5).fill_color(Color.Red).line_color(Color.Black)
  val circle = new GlyphRenderer().data_source(source).glyph(glyph)

  plot.renderers := List[Renderer](xaxis, yaxis, circle)

  val document = new Document(plot)
  val html = document.save("sample.html")
  println(s"Wrote ${html.file}. Open ${html.url} in a web browser.")
  html.view()

  def timeSeries(dates: Seq[Double], values: Seq[Double]): Unit = {
    object source extends ColumnDataSource {
      val times = column(dates)
      val y     = column(values)
    }

    import source.{times, y}

    val xdr = new DataRange1d()
    val ydr = new DataRange1d()

    val circle = new Circle().x(times).y(y).fill_color(Color.Red).size(5).line_color(Color.Black)

    val renderer = new GlyphRenderer()
      .data_source(source)
      .glyph(circle)

    val plot = new Plot().x_range(xdr).y_range(ydr)

    val xaxis = new DatetimeAxis().plot(plot)
    val yaxis = new LinearAxis().plot(plot)
    plot.below <<= (xaxis :: _)
    plot.left <<= (yaxis :: _)

    val pantool = new PanTool().plot(plot)
    val wheelzoomtool = new WheelZoomTool().plot(plot)

    plot.renderers := List(xaxis, yaxis, renderer)
    plot.tools := List(pantool, wheelzoomtool)

    val document = new Document(plot)
    val html = document.save("dateaxis.html")
    html.view()
  }
}
