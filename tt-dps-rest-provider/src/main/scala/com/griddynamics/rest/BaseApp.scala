package com.griddynamics.rest

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

trait BaseApp extends App {
  def basePath():String = sys.env.getOrElse("BASEPATH",throw new Error("basepath not present"))
  def port():Int = Integer.parseInt(sys.env.getOrElse("PORT",throw new Error("port not present")))

  val server = {
    val serv = new Server(port())
    val ctx = new WebAppContext()
    ctx.setContextPath(basePath())
    ctx.setResourceBase("/tmp")
    ctx.addEventListener(new ScalatraListener)
    serv.setHandler(ctx)
    serv
  }

  protected def startServer(): Unit = {
    server.start()
    server.join()
  }
}
