package io.keepcoding.jdebran.speed

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import io.keepcoding.jdebran.domain.{Cliente, Geolocalizacion, Transaccion}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scalaj.http.Http

import scala.collection.mutable

object MetricasSparkStreaming {

  def run(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Práctica - Speed Layer - Spark Streaming")
      .getOrCreate()

    /* Tarea 1
     * Crear un productor de kafk y consumidor de kafka en spark streaming del tópico transacciones
     */
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )
    val stream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array("transacciones"), kafkaParams))

    val transaccionesStream = stream.map(record => record.value)

    transaccionesStream.foreachRDD { rdd =>
      import spark.implicits._

      if (!rdd.isEmpty) {
        val transaccionesDF = rdd.map(row => {
          val outputFormat = new SimpleDateFormat("MM/dd/yy HH:mm")
          val cols = row.split(",")
          (new Timestamp(outputFormat.parse(cols(0).trim).getTime), cols(1).trim, cols(2).trim.toInt, cols(3).trim, cols(4).trim, cols(5).trim,
            cols(6).trim.toInt, new Timestamp(outputFormat.parse(cols(7).trim).getTime), cols(8).trim.toDouble, cols(9).trim.toDouble, cols(10).trim)
        }).toDF("Transaction_date", "Product", "Price", "Payment_Type", "Name", "City",
          "Account_Created", "Last_Login", "Latitude", "Longitude", "Description")
        transaccionesDF.cache.show

        /* Tarea 2
         * Implementación de las mismas tareas del sprint 2
         */
        val clientes = transaccionesDF.groupBy("Name")
          .agg(collect_list("Account_Created").alias("Accounts"))
          .withColumn("DNI", row_number().over(Window.orderBy("Name")))
          .select($"DNI", $"Name".alias("nombre"), $"Accounts".alias("cuentaCorriente"))
          .as[Cliente]

        val transacciones = transaccionesDF.map(row => Transaccion(row.getAs[Timestamp]("Transaction_date"),
          row.getAs[Integer]("Account_Created"), row.getAs[Integer]("Price").toDouble,
          row.getAs[String]("Description"), row.getAs[String]("Payment_Type"),
          Geolocalizacion(row.getAs[Double]("Latitude"), row.getAs[Double]("Longitude"),
            row.getAs[String]("City"), "N/A")
        ))

        val checkValue = udf {
          (array: mutable.WrappedArray[Int], value: Int) => array.contains(value)
        }

        val joinDF = clientes.join(transacciones,
          checkValue(clientes.col("cuentaCorriente"), transacciones.col("cuentaCorriente")))

        joinDF.cache.show

        println("Task 1")
        val task1 = joinDF.groupBy("geolocalizacion.ciudad").count.orderBy(desc("count"))
        task1.cache.show
        task1.rdd.foreachPartition(writeToKafka("task1"))

        println("Task 2")
        val task2 = joinDF.filter($"importe" >= 5000)
        task2.cache.show
        task2.rdd.foreachPartition(writeToKafka("task2"))

        println("Task 3")
        val task3 = joinDF.filter($"geolocalizacion.ciudad" === args(1)).groupBy("DNI").count.orderBy(desc("count"))
        task3.cache.show
        task3.rdd.foreachPartition(writeToKafka("task3"))

        println("Task 4")
        val task4 = joinDF.filter($"descripcion" === "Restaurant" || $"descripcion" === "Cinema" || $"descripcion" === "Sports")
        task4.cache.show
        task4.rdd.foreachPartition(writeToKafka("task4"))

        println("Task 5")
        val task5 = joinDF.filter($"fecha".between(date_sub(current_date(), args(2).toInt), current_date())).orderBy(desc("fecha"))
        task5.cache.show
        task5.rdd.foreachPartition(writeToKafka("task5"))

        println("Task 6")
        val task6 = transacciones.map { t =>
          val response = Http("https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=" + t.geolocalizacion.latitud + "&lon=" + t.geolocalizacion.longitud)
          val json = scala.util.parsing.json.JSON.parseFull(response.asString.body)
          val country = json.get.asInstanceOf[Map[String, Any]].get("address").get.asInstanceOf[Map[String, Any]].get("country") match {
            case Some(e) => e.toString
          }
          Transaccion(t.fecha, t.cuentaCorriente, t.importe, t.descripcion, t.tarjetaCredito,
            Geolocalizacion(t.geolocalizacion.latitud, t.geolocalizacion.longitud, t.geolocalizacion.ciudad, country))
        }
        task6.cache.show
        task6.toDF.rdd.foreachPartition(writeToKafka("task6"))

        /* Tarea 4
         * Analizar y desarrollar dos métricas extra que aporten valor al usuario.
         * Estas métricas se guardan en formato texto en un fichero
         */
        println("Metrics 1")
        val metrics1 =  joinDF.groupBy("DNI").agg(sum("importe"))
        metrics1.cache.show
        metrics1.rdd.foreachPartition(writeToKafka("metrics1"))
        metrics1.rdd.coalesce(1).saveAsTextFile(args(0) + "/metrics1")

        println("Metrics 2")
        val metrics2 = task6.groupBy("geolocalizacion.pais").count.orderBy(desc("count"))
        metrics2.cache.show
        metrics2.rdd.foreachPartition(writeToKafka("metrics2"))
        metrics2.rdd.coalesce(1).saveAsTextFile(args(0) + "/metrics2")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  /* Tarea 3
   * Creación de un sumidero que envíe de vuelta a kafka los resultados finales
   */
  def writeToKafka(topic: String)(partitionOfRecords: Iterator[Row]): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](properties)

    partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](topic, data.toString)))
    producer.flush()
    producer.close()
  }

}
