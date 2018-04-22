package io.keepcoding.jdebran

import io.keepcoding.jdebran.batch.MetricasSparkSQL
import org.apache.log4j.{Level, Logger}

object BatchApplication {

  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    if (args.length == 5) {
      MetricasSparkSQL.run(args)
    }
    else {
      println("Se está intentando arrancar el job de Spark sin los parámetros correctos")
      println("Parámetro 1 => Path con el fichero de origen")
      println("Parámetro 2 => Path donde guardar los ficheros")
      println("Parámetro 3 => Formato ficheros para guardar")
      println("Parámetro 4 => Ciudad para agrupar")
      println("Parámetro 5 => Número de días")
    }
  }
}
