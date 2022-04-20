package com.griddynamics.common

package object pipeline {

  trait Node {

    def name: String
    def children: Seq[Node]
    def execute: () => Any

    private def downstream(nodes: Seq[Node]): Node = {
      val thisNode = this
      new Node {
        override def name: String = thisNode.name

        override def children: Seq[Node] = thisNode.children ++ nodes

        override def execute: () => Any = thisNode.execute
      }
    }

    def >>(nodes: Seq[Node]): Node = downstream(nodes)
    def >>(nodes: Node): Node = downstream(Seq(nodes))

    override def toString: String = name
  }

  object Node {
    def unapply(arg: Node): Option[(String, Seq[Node])] = Some(
      (arg.name, arg.children)
    )
  }

  private[pipeline] case class RootNode() extends Node {
    override val name: String = "root"
    override val children: Seq[Node] = Seq.empty
    override val execute: () => Any = () => None
  }

  class DAG(val name: String, private val rootNode: Node = RootNode()) {

    def withNodeStructure(constructor: Node => Node): DAG = {
      new DAG(name, constructor(rootNode))
    }

    lazy val flatten: Seq[(Int, _ <: Node)] = {
      def inner(dept: Int, nodes: Seq[_ <: Node]): Seq[(Int, _ <: Node)] = {
        nodes flatMap { node =>
          if (node.isInstanceOf[RootNode]) {
            inner(dept, node.children) //skip dummy
          } else if (node.children.isEmpty) {
            Seq((dept, node))
          } else {
            Seq((dept, node)) ++ inner(dept + 1, node.children)
          }
        }
      }
      val res = inner(0, Seq(rootNode))
      res.sortBy(_._1)
    }

    private def maxDeptPerNode: Seq[(Int, Node)] = flatten
      .groupBy(_._2.name)
      .values
      .map(node => node.maxBy(_._1))
      .toSeq
      .sortBy(_._1)

    def evaluate(): Seq[Any] = {
      maxDeptPerNode.map(tuple => tuple._2.execute())
    }

    override def toString: String = flatten.mkString
  }

  object DAG {
    def apply(name: String): DAG = new DAG(name)
  }
}
