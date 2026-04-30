package examen

import examen_estructura.Examen._
import org.apache.spark.SparkContext
import utils.TestInit

class examenTest extends TestInit {

  implicit val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  "Ejercicio 1" should "Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente" in {

    // Este test comprueba filtro + orden + selección de la columna nombre.
    val estudiantes = Seq(
      ("Ana", 23, 9.0),
      ("Luis", 21, 7.5),
      ("Pedro", 22, 8.5),
      ("Maria", 20, 9.5)
    ).toDF("nombre", "edad", "calificacion")

    val out = ejercicio1(estudiantes).collect().map(_.getString(0))

    out shouldBe List("Maria", "Ana", "Pedro")
  }

  "Ejercicio 2" should "Devuelve los datos paritarios" in {

    // Este test comprueba que la UDF devuelve Par o Impar.
    val numeros = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")

    val out = ejercicio2(numeros).collect().map(_.getString(0))

    out shouldBe List("Impar", "Par", "Impar", "Par", "Impar", "Par", "Impar", "Par", "Impar", "Par")
  }

  "Ejercicio 3" should "calcula el promedio de calificaciones por estudiante" in {

    // Este test comprueba join + groupBy + avg.
    val estudiantes = Seq(
      (1, "Ana"),
      (2, "Luis"),
      (3, "Pedro"),
      (4, "Maria")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matematicas", 9.0),
      (1, "Historia", 8.5),
      (2, "Matematicas", 7.5),
      (3, "Historia", 8.0),
      (4, "Matematicas", 9.5),
      (4, "Historia", 9.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val out = ejercicio3(estudiantes, calificaciones).collect().map(x => (x.get(0), x.get(1), x.get(2)))

    out shouldBe List((1, "Ana", 8.75), (2, "Luis", 7.5), (3, "Pedro", 8.0), (4, "Maria", 9.25))
  }

  "Ejercicio 4" should "cuenta la cantidad de ocurrencias de cada palabra" in {

    // Este test comprueba el conteo con RDD.
    val palabras = List("spark", "hadoop", "spark", "hive", "spark", "hadoop", "hive")

    // Paso 1: ejecutar la función
    // Paso 2: ordenar para comparar siempre igual
    val out = ejercicio4(palabras).collect().sorted

    out shouldBe Array(("hadoop", 2), ("hive", 2), ("spark", 3))
  }

  "Ejercicio 5" should "calcula el ingreso total" in {

    // Este test lee el CSV y luego llama al ejercicio5.
    // Si guardas ventas.csv en src/test/resources, primero usará esa ruta.
    // Si no, usa la ruta del archivo subido en este entorno.
    val path = Option(getClass.getClassLoader.getResource("ventas.csv"))
      .map(_.getPath)
      .getOrElse("/mnt/data/ventas.csv")

    val ventas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    // Nota: si quieres usar ventas2.csv, añade .option("delimiter", ";")
    val out = ejercicio5(ventas).collect().map(x => (x.getInt(0), x.getDouble(1)))

    out.toList shouldBe List(
      (101, 460.0),
      (102, 405.0),
      (103, 280.0),
      (104, 800.0),
      (105, 570.0),
      (106, 425.0),
      (107, 396.0),
      (108, 486.0),
      (109, 540.0),
      (110, 494.0)
    )
  }

}
