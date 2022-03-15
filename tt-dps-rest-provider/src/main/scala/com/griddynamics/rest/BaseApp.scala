package com.griddynamics.rest

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener
import com.griddynamics.common.ConfigUtils.servlets

trait BaseApp extends App {

  def basePath():String = servlets.generator.basePath
  def port():Int = servlets.generator.port

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
