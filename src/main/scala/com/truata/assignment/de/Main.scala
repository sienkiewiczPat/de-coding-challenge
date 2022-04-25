package com.truata.assignment.de

import com.truata.assignment.de.Solver.process
import scala.io.StdIn

object Main {
    def main(args: Array[String]): Unit = {
      Iterator.continually(Option(StdIn.readLine()))
        .takeWhile(_.nonEmpty)
        .foreach {
          x => x map process
        }
    }
}
