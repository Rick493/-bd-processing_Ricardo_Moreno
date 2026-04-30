package examen_estructura

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Examen {

  /** Ejercicio 1
   * Objetivo: filtrar alumnos con nota > 8 y devolver sus nombres ordenados por nota desc.
   */
  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Paso 1: mostrar el esquema (sirve para ver tipos y columnas)
    estudiantes.printSchema()

    // Paso 2: filtrar solo los que tienen nota mayor que 8
    // Paso 3: ordenar por calificacion de mayor a menor
    // Paso 4: devolver solo la columna nombre (es lo que pide el test)
    estudiantes
      .filter(functions.col("calificacion") > 8)
      .select("nombre", "calificacion")
      .orderBy(functions.col("calificacion").desc)
      .select("nombre")
  }

  /** Ejercicio 2
   * Objetivo: usar una UDF para decir si cada numero es Par o Impar.
   */
  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Paso 1: crear la UDF
    val parOImpar = functions.udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")

    // Paso 2: aplicar la UDF sobre la columna numero
    // Paso 3: dejar solo el resultado porque el test lee la primera columna
    numeros.select(parOImpar(functions.col("numero")).alias("resultado"))
  }

  /** Ejercicio 3
   * Objetivo: unir alumnos con notas y sacar el promedio por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    // Paso 1: hacer el join por id
    // Paso 2: agrupar por id y nombre
    // Paso 3: calcular el promedio y ordenar por id para que salga estable
    estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .groupBy(estudiantes("id"), estudiantes("nombre"))
      .agg(functions.avg("calificacion").alias("promedio"))
      .orderBy(functions.col("id"))
  }

  /** Ejercicio 4
   * Objetivo: contar cuantas veces aparece cada palabra usando RDD.
   */
  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    // Paso 1: pasar la lista a RDD
    // Paso 2: convertir cada palabra en (palabra, 1)
    // Paso 3: sumar por clave
    spark.sparkContext
      .parallelize(palabras)
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)
  }

  /** Ejercicio 5
   * Objetivo: calcular el ingreso total por producto = cantidad * precio_unitario.
   */
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {
    // Paso 1: crear la columna ingreso por fila
    // Paso 2: agrupar por producto
    // Paso 3: sumar el ingreso total y ordenar por id_producto
    ventas
      .withColumn("ingreso", functions.col("cantidad") * functions.col("precio_unitario"))
      .groupBy("id_producto")
      .agg(functions.sum("ingreso").alias("ingreso_total"))
      .orderBy(functions.col("id_producto"))
  }

}
