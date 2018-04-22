package io.keepcoding.jdebran.domain

import java.sql.Timestamp

case class Geolocalizacion(latitud: Double, longitud: Double, ciudad: String, pais: String)

case class Transaccion(fecha: Timestamp, cuentaCorriente: Integer, importe: Double, descripcion: String, tarjetaCredito: String, geolocalizacion: Geolocalizacion)

case class Cliente(DNI: Integer, nombre: String, cuentaCorriente: List[Integer])
