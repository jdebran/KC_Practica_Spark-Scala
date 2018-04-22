package io.keepcoding.jdebran.batch

import io.keepcoding.jdebran.domain.{Cliente, Geolocalizacion, Transaccion}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import java.sql.Timestamp
import scalaj.http.Http
import scala.collection.mutable

object MetricasSparkSQL {

  def run(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Práctica - Batch Layer - Spark SQL")
      .getOrCreate()

    import spark.implicits._

    val csvFile = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("ignoreLeadingWhiteSpace", "true")
      .option("ignoreTrailingWhiteSpace", "true")
      .option("timestampFormat", "MM/dd/yy hh:mm")
      .load(args(0))

    csvFile.printSchema
    csvFile.show(10)

    //Cliente(DNI: Integer, nombre: String, cuentaCorriente: List[Integer])
    val clientes = csvFile.groupBy("Name")
      .agg(collect_list("Account_Created").alias("Accounts"))
      .withColumn("DNI", row_number().over(Window.orderBy("Name")))
      .select($"DNI", $"Name".alias("nombre"), $"Accounts".alias("cuentaCorriente"))
      .as[Cliente]

    clientes.printSchema
    clientes.show(10)

    //Transaccion(fecha: Timestamp, cuentaCorriente: Integer, importe: Double, descripcion: String, tarjetaCredito: String, geolocalizacion: Geolocalizacion)
    //Geolocalizacion(latitud: Double, longitud: Double, ciudad: String, pais: String)
    val transacciones = csvFile.map(row => Transaccion(row.getAs[Timestamp]("Transaction_date"),
      row.getAs[Integer]("Account_Created"), row.getAs[Integer]("Price").toDouble,
      row.getAs[String]("Description"), row.getAs[String]("Payment_Type"),
      Geolocalizacion(row.getAs[Double]("Latitude"), row.getAs[Double]("Longitude"),
        row.getAs[String]("City"), "N/A")
    ))

    transacciones.printSchema
    transacciones.show(10)

    val checkValue = udf {
      (array: mutable.WrappedArray[Int], value: Int) => array.contains(value)
    }

    val joinDF = clientes.join(transacciones,
      checkValue(clientes.col("cuentaCorriente"), transacciones.col("cuentaCorriente")))

    joinDF.printSchema
    joinDF.show(10)

    /* Tarea 1
     * Agrupa todos los clientes por ciudad.
     * El objetivo sería contar todas las transacciones ocurridas por ciudad
     */
    println("Task 1")
    val task1 = joinDF.groupBy("geolocalizacion.ciudad").count.orderBy(desc("count"))
    task1.show(10)

    /* Tarea 2
     * Encuentra aquellos clientes que hayan realizado pagos superiores a 5000
     */
    println("Task 2")
    val task2 = joinDF.filter($"importe" >= 5000)
    task2.show(10)

    /* Tarea 3
     * Obtén todas las transacciones agrupadas por cliente cuya ciudad sea X
     */
    println("Task 3")
    val task3 = joinDF.filter($"geolocalizacion.ciudad" === args(3)).groupBy("DNI").count.orderBy(desc("count"))
    task3.show(10)

    /* Tarea 4
     * Filtra todas las operaciones cuya categoría sea Ocio
     */
    println("Task 4")
    val task4 = joinDF.filter($"descripcion" === "Restaurant" || $"descripcion" === "Cinema" || $"descripcion" === "Sports")
    task4.show(10)

    /* Tarea 5
     * Obtén las últimas transacciones de cada cliente en los úlitmos n días
     */
    println("Task 5")
    val task5 = joinDF.filter($"fecha".between(date_sub(current_date(), args(4).toInt), current_date())).orderBy(desc("fecha"))
    task5.show(10)

    /* Tarea 6 [Opcional]
     * Encuentra por cada transacción a través de su longitud y latitud cual es el país a donde pertenece
     * la transacción y guárdalo en el campo país
     */
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
    task6.show(10)

    /*
     * Los resultados se almacenarán combinando diferentes formatos de ficheros (CSV, Avro, Parquet, etc)
     */
    val formato = if(args(2) == "avro") "com.databricks.spark.avro" else args(2)
    task1.write.option("header", "true").mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task1")
    //task2.write.option("header", "true").mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task2")
    task3.write.option("header", "true").mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task3")
    //task4.write.option("header", "true").mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task4")
    //task5.write.option("header", "true").mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task5")
    task6.map(row => (row.fecha, row.cuentaCorriente, row.importe, row.descripcion, row.tarjetaCredito,
      row.geolocalizacion.latitud, row.geolocalizacion.longitud, row.geolocalizacion.ciudad, row.geolocalizacion.pais))
      .toDF("fecha", "cuentaCorriente", "importe", "descripcion", "tarjetaCredito", "latitud", "longitud", "ciuad", "pais")
      .write.mode(SaveMode.Overwrite).format(formato).save(args(1) + "/" + args(2) + "/task6")
  }

}
