package com.griddynamics.rest

import org.scalatra.{LifeCycle, ScalatraServlet}

import javax.servlet.ServletContext

trait Bootstrap extends LifeCycle{

 override def init(context: ServletContext): Unit = {
   servlets().foreach(servlet => context.mount(servlet,"/*"))
  }

  override def destroy(context: ServletContext): Unit = super.destroy(context)

  def servlets(): Seq[ScalatraServlet] = Seq.empty
}
