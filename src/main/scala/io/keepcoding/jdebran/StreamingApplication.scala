package io.keepcoding.jdebran

import io.keepcoding.jdebran.speed.MetricasSparkStreaming
import org.apache.log4j.{Level, Logger}

object StreamingApplication {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    if (args.length == 3) {
      MetricasSparkStreaming.run(args)
    }
    else {
      println("Se está intentando arrancar el job de Spark sin los parámetros correctos")
      println("Parámetro 1 => Path donde guardar los ficheros")
      println("Parámetro 2 => Ciudad para agrupar")
      println("Parámetro 3 => Número de días")
    }
  }

}
