package examen_estructura

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Examen {

  /** Ejercicio 1
   * * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * * estudiantes (nombre, edad, calificación).
   * * Realiza las siguientes operaciones.
   * * Muestra el esquema del DataFrame.
   * * Filtra los estudiantes con una calificación mayor a 8.
   * * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente*/

  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    estudiantes.printSchema()
    estudiantes
      .filter(functions.col("calificacion") > 8)
      .select("nombre", "calificacion")
      .orderBy(functions.col("calificacion").desc)
      .select("nombre")
  }

  /** Ejercicio 2
   * Pregunta: Define una función que determine si un número es par o impar.
   * * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   * */

  def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {
   val parOImpar = functions.udf((n: Int) => if (n % 2 == 0) "Par" else "Impar")
    numeros.select(parOImpar(functions.col("numero")).alias("resultado"))
  }

  /** Ejercicio 3
   * Pregunta: Dado dos DataFrames, uno con información de estudiantes (id, nombre)
   * * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   * */

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {
    estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))
      .groupBy(estudiantes("id"), estudiantes("nombre"))
      .agg(functions.avg("calificacion").alias("promedio"))
      .orderBy(functions.col("id"))
  }

  /** Ejercicio 4
   * Pregunta: Crea un RDD a partir de una lista de palabras y
   * cuenta la cantidad de ocurrencias de cada palabra.
   */

  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    spark.sparkContext
      .parallelize(palabras)
      .map(palabra => (palabra, 1))
      .reduceByKey(_ + _)
  }

  /** Ejercicio 5
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * * y calcula el ingreso total (cantidad * precio_unitario) por producto,
   * * ordenados por id_producto
   * */
  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {

    ventas
      .withColumn("ingreso", functions.col("cantidad") * functions.col("precio_unitario"))
      .groupBy("id_producto")
      .agg(functions.sum("ingreso").alias("ingreso_total"))
      .orderBy(functions.col("id_producto"))
  }

}
