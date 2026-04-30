package sesion5

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions._

import java.util.Calendar
import scala.math.BigDecimal.RoundingMode

object Ejercicios5Estructutra {

  /**
   * Crear un RDD apartir del fichero (README.md ) ejecutaracciones/transformaciones para contestar:
   *
   * • ¿Cuántas líneas tiene el fichero?
   *
   * • ¿En cuántas líneas aparece la palabra “scala”?
   *
   * • Imprimir por pantalla el numero de palabras de las 5 primeras lineas
   *
   * Devolver el resultado en forma de tupla para hacer match con el test
   */
  object Ejercicio1 {
    def ejercicio1(fileRDD: RDD[String])(implicit sc: SparkContext): (Int, Int, Int) = {
      //¿Cuántas líneas tiene el fichero?
      val numLineas = fileRDD.count().toInt
      //¿En cuántas líneas aparece la palabra “scala”?
      val numVecesScala = fileRDD.filter(line => line.toLowerCase().contains("scala")).count().toInt
      val numVecesScala2 = fileRDD.filter(_.toLowerCase contains "scala").count().toInt
      val numVecesScalaSinEspacios = fileRDD.filter(_.toLowerCase.split("\\s") contains "scala").count().toInt
      println("numVecesScala: " + numVecesScala + "numVecesScala2: " + numVecesScala2 + "numVecesScalaSinEspacios: " + numVecesScalaSinEspacios)
      val numPalabras = fileRDD.take(5).map(_.split("\\s").length).sum
      val numPalabras2 = sc.parallelize(fileRDD.take(5)).flatMap(_.split(" ")).count.toInt //No cuenta los #
      println((numLineas, numVecesScala, numPalabras))
      (numLineas, numVecesScala, numPalabras)

    }
  }

  /**
   * Definir función plural
   * <ul>
   * <li> Pasar wordsRDD a plural.
   * <li> Calcular la longitud de cada palabra
   * <li> Trasformar los elementos en pares de elementos (palabra, 1) Contar ocurrencias de palabras.
   * es necesario que la lista este ordenada para hacer match con el test
   * <li> Igual que antes, pero ordenados por el numero de apariciones y usando GroupByKey
   */
  object Ejercicio2 {

    def pluralPalabras(wordsRDD: RDD[String]): RDD[String] = {
      val vocales = Set('a', 'e', 'i', 'o', 'u')
      val vocalesString = "aeiou"
      wordsRDD.map {
        palabra =>
          if (palabra.endsWith("s")) {
            palabra
          } else if (vocalesString.contains(palabra.last)) {
            palabra + "s"
          } else {
            palabra + "es"
          }
      }

      wordsRDD.map {
        case w if w.endsWith("s") => w
        case w if w.endsWith("l") => w + "es"
        case w => w + "s"
      }

      wordsRDD.map {
        case word if vocales.contains(word.last) => word :+ 's'
        case word if "s".contains(word.last) => word
        case word => word + "es"
      }


    }

    def longitudPalabras(wordsRDD: RDD[String]): RDD[Int] = wordsRDD.map(_.length)

    // Podeis que usar reduceByKey()
    def contarPalabras(wordsRDD: RDD[String]): RDD[(String, Int)] = {
      wordsRDD.map(palabra => (palabra, 1))
        .reduceByKey(_ + _)
        .sortByKey()
    }

    def contarPalabrasPorFrecuencia(wordsRDD: RDD[String]): RDD[(String, Int)] = {
      wordsRDD.map(palabra => (palabra, 1))
        .groupByKey()
        .mapValues(_.size)
    }

    // El objetivo en contarPalabras es:
    // Convertimos pato => (pato,1), osos => (osos,1) ....
    // Agrupamos por Key (en este caso es la palabra): (caracoles,1)(caracoles, 1)...
    // Sumamos por size
  }


  /**
   * La clase LogRecord es la estructura de cada linea de apache.log
   * Los metodos parseLogLine y parseLogsFile estan para ayudarte a conseguir leer cada linea del fichero.
   * Una vez leido el fichero:
   * <li> calcularMaxMinAvgDeContentSize
   * <li> Top10Endpoints mas visitados
   * <li> Top10Endpoints mas visitados con HTTPCode != 200
   * <li> Agrupar numero de visitas por codigo de respuesta
   */

  case class LogRecord(host: String,
                       date_time: Calendar,
                       method: String,
                       endpoint: String,
                       protocol: String,
                       response_code: Int,
                       content_size: Long)

  object Ejercicio3 {

    def parseLogLine(line: String): Option[LogRecord] = {
      val logPattern = """^(\S+) \[(.*?)\] "(.*?) (.*?) (.*?)" (\d{3}) (\S+)$""".r

      line match {
        case logPattern(host, dateTime, method, endpoint, protocol, response, size) =>
          try {
            val contentSize = if (size == "-") 0L else size.toLong
            Some(LogRecord(host, Calendar.getInstance(), method, endpoint, protocol, response.toInt, contentSize))
          } catch {
            case _: Exception => None
          }
        case _ => None
      }
    }

    def parseLogsFile(linesRDD: RDD[String]): RDD[LogRecord] = {
      linesRDD.flatMap(parseLogLine)
    }

    def calcularEstadisticasContentSize(logsRDD: RDD[LogRecord]): (Long, Long, Double) = {
      val sizes = logsRDD.map(_.content_size)
      (sizes.min(), sizes.max(), BigDecimal(sizes.sum() / sizes.count().toDouble).setScale(2, RoundingMode.HALF_UP).toDouble)
    }

    def top10Endpoints(logsRDD: RDD[LogRecord]): Array[(String, Int)] = {
      logsRDD
        .map(log => (log.endpoint, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(10)
    }

    def top10EndpointsFailures(logsRDD: RDD[LogRecord]): Array[(String, Int)] = {
      logsRDD
        .filter(log => log.response_code != 200)
        .map(log => (log.endpoint, 1))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(10)
    }

    def visitasPorCodigo(logsRDD: RDD[LogRecord]): RDD[(Int, Int)] = {
      logsRDD
        .map(log => (log.response_code, 1))
        .reduceByKey(_ + _)
        .sortByKey()
    }

  }

  /**
   * Procesar el DataSetLaLiga.txt
   * <li> Crear una clase que identifique cada linea de log
   * <ul>
   * <li> Calcular los goles que ha marcado el Betis
   * <li> Numero de partidos que acabo en empate
   * <li> Top 5 equipos con más goles marcados en total
   * <li> Qué equipo es el mejor local en los últimos 5 años?
   * </ul>
   */
  object Ejercicio4 {

    case class Partido(
                        id: Int,
                        temporada: String,
                        jornada: Int,
                        equipoLocal: String,
                        equipoVisitante: String,
                        golesLocal: Int,
                        golesVisitante: Int,
                        fechaPartido: String,
                        timestamp: Double
                      )

    def parseLineaLaLiga(line: String): Option[Partido] = {
      val campos = line.split("::")
      if (campos.length == 9) {
        try {
          Some(Partido(
            id = campos(0).toInt,
            temporada = campos(1),
            jornada = campos(2).toInt,
            equipoLocal = campos(3),
            equipoVisitante = campos(4),
            golesLocal = campos(5).toInt,
            golesVisitante = campos(6).toInt,
            fechaPartido = campos(7),
            timestamp = campos(8).toDouble
          ))
        } catch {
          case _: Exception => None
        }
      } else None
    }

    def golesDelBetis(partidosRDD: RDD[Partido]): Int = {
      partidosRDD.map {
        partido =>
          if (partido.equipoLocal == "Betis") partido.golesLocal
          else if (partido.equipoVisitante == "Betis") partido.golesVisitante
          else 0
      }.sum().toInt

      //    val golesBetis = partidosRDD
      //      .filter(p => p.equipoLocal == "Betis" || p.equipoVisitante == "Betis")
      //      .map(p => if (p.equipoLocal == "Betis") p.golesLocal else p.golesVisitante)
      //      .sum()
      //      .toInt
    }

    def numeroEmpates(partidosRDD: RDD[Partido]): Long = {
      partidosRDD.filter(x => x.golesVisitante == x.golesLocal).count()
    }

    def top5EquiposConMasGoles(partidosRDD: RDD[Partido]): Array[(String, Int)] = {
      val golesPorEquipo = partidosRDD.flatMap(p => Seq((p.equipoLocal, p.golesLocal), (p.equipoVisitante, p.golesVisitante)))
      golesPorEquipo
        .reduceByKey(_ + _)
        .sortBy({
          case (_, goles) => -goles
        })
        .take(5)

      val golesLocales = partidosRDD.map(x => (x.equipoLocal, x.golesLocal)).reduceByKey(_ + _)
      val golesVisitantes = partidosRDD.map(x => (x.equipoVisitante, x.golesVisitante)).reduceByKey(_ + _)

      golesLocales.join(golesVisitantes).map {
          case (equipo, (golesLocal, golesVisitante)) => (equipo, golesLocal + golesVisitante)
        }
        .sortBy(_._2, ascending = false)
        .take(5)


      val golesLocales2 = partidosRDD.map(partido => (partido.equipoLocal, partido.golesLocal))
      val golesVisitantes2 = partidosRDD.map(partido => (partido.equipoVisitante, partido.golesVisitante))

      golesLocales2.union(golesVisitantes2) // unimos ambos RDDs
        .reduceByKey(_ + _) // sumamos goles por equipo
        .sortBy({ case (_, totalGoles) => totalGoles }, ascending = false) // orden descendente
        .take(5) // top 5 equipos

    }

    def mejorEquipoLocalUltimasTemporadas(partidosRDD: RDD[Partido], ultimasTemporadas: Seq[String]): (String, Int) = {
      partidosRDD
        .filter(p => ultimasTemporadas.contains(p.temporada))
        .map(p =>
          (p.equipoLocal,
            if (p.golesLocal > p.golesVisitante) 3
            else if (p.golesLocal == p.golesVisitante) 1
            else 0
          ))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .first()
    }
  }


  /**
   * Ejercicio 5
   *
   * Registrar DataSetLaLiga.txt como Tabla usando y usando SQL / Core
   * <ul>
   * <li> ¿Quién ha estado más temporadas en 1a División: Sporting u Oviedo?
   * <li> ¿Cuál es el record de goles como visitante en una temporada del Oviedo?
   * <li> ¿En qué temporada se marcaron más goles en Asturias? (entre el Oviedo y el Sporting como locales)
   * <li> Goles marcados y recibidos por el Sporting jugando de local
   * </ul>
   */
  object Ejercicio5 {

    case class Partido(
                        id: Int,
                        temporada: String,
                        jornada: Int,
                        equipoLocal: String,
                        equipoVisitante: String,
                        golesLocal: Int,
                        golesVisitante: Int,
                        fechaPartido: String,
                        timestamp: Double
                      )

    def fileToPartidos(ds: Dataset[String])(implicit spark: SparkSession): Dataset[Partido] = {
      import spark.implicits._

      ds.filter(line => line.contains("::"))
        .map { x =>
          val p = x.split("::")
          Partido(
            p(0).toInt,
            p(1),
            p(2).toInt,
            p(3),
            p(4),
            p(5).toInt,
            p(6).toInt,
            p(7),
            p(8).toDouble
          )
        }
    }

    // Metodo necesario en cada uno de las funciones definidas abajo.
    def generaView(ds: Dataset[Partido]): Unit = ds.createOrReplaceTempView("partidos")

    def masTemporadaPrimeraDiv(ds: Dataset[Partido]): String = {
      val temporadaSporting = ds.filter(col("equipoLocal") === "Sporting de Gijon")
        .select("temporada")
        .distinct()
        .orderBy(desc("temporada"))
        .count()

      val temporadaOviedo = ds.filter(col("equipoLocal") === "Oviedo")
        .select("temporada")
        .distinct()
        .orderBy(desc("temporada"))
        .count()

      if (temporadaSporting > temporadaOviedo) "Sporting" else "Oviedo"
    }

    def maximosOviedo(ds: Dataset[Partido]): Int = {
      ds.filter(col("equipoVisitante") === "Real Oviedo")
        .groupBy("temporada")
        .agg(sum("golesVisitante").as("sumaGoles"))
        .agg(functions.max("sumaGoles"))
        .first().getLong(0).toInt
    }

    def maximosTemporada(ds: Dataset[Partido]): Int =
      ds
        .filter(col("equipoLocal") === "Real Oviedo" || col("equipoLocal") === "Sporting de Gijon")
        .withColumn("totales", col("golesLocal") + col("golesVisitante"))
        .groupBy("temporada")
        .sum("totales")
        .agg(functions.max("sum(totales)"))
        .first()
        .getLong(0).toInt

    def golesEncajados(ds: Dataset[Partido]): Array[Row] = {
      val tablaGoles: DataFrame = ds.filter(col("equipoLocal") === "Sporting de Gijon")
        .agg(
          sum("golesLocal").alias("goles_marcados"),
          sum("golesVisitante").alias("goles_recibidos")
        )
      tablaGoles.show()
      tablaGoles.collect()
    }
  }

  /**
   * Ejercicio 6
   * <ul>
   * <li> Crear un DataFrame a partir del fichero natality.csv
   * <li> Obtener top 10 estados donde nacieron más niños en 2003
   * <li> Obtener top 3 de la media de peso de los niños por año y estado
   * <li> Peso medio de los bebés por edad de la madre
   * <li> Utilizando SQL
   * <li> Responder a las misma preguntas del anterior apartado
   * <li> Crear un DataSet a partir del DataFrame
   * <li> Obtener los 3 meses de 2005 en que nacieron más niños
   * </ul>
   */
  object Ejercicio6 {
    def top10Paises(natalityDF: DataFrame): Dataset[Row] = {
      natalityDF
        .filter(col("year") === 2003)
        .groupBy("state")
        .count()
        .orderBy(desc("count"))
        .limit(10)
    }

    def weightAverage(natalityDF: DataFrame): Dataset[Row] = {
      natalityDF
        .groupBy("year", "state")
        .agg(round(avg("weight_pounds"),2).alias("avg_weight"))
        .orderBy(desc("avg_weight"))
        .limit(3)
    }

    def weightAverageByMumAge(natalityDF: DataFrame): Dataset[Row] = {
      natalityDF
        .groupBy("mother_age")
        .agg(round(avg("weight_pounds"),2).alias("avg_weight"))
        .orderBy(desc("avg_weight"))

    }

    def top10PaisesSQl(natalityDF: DataFrame)(implicit sparkSession: SparkSession): Unit = {
      natalityDF.createOrReplaceTempView("natality")

      sparkSession.sql(" SELECT country, count(*) as total_birth FROM natality" +
        "WHERE year = 2003 GROUP BY state ORDER BY total_birth desc LIMIT 10")

    }

    def weightAverageBySQL(natalityDF: DataFrame)(implicit sparkSession: SparkSession): Unit = {
      natalityDF.createOrReplaceTempView("natality")
      sparkSession.sql(" SELECT year, state, avg(weight_pounds) as avg_weight from natality" +
        "group by year, state, order by year, state")
    }

    def weightAverageByMumAgeSQL(natalityDF: DataFrame)(implicit sparkSession: SparkSession): Unit = {
      natalityDF.createOrReplaceTempView("natality")
      sparkSession.sql("select mother_age, avg(weight_pounds) as avg_weight from natality" +
        "group by mother_age")
    }

    case class NatalitySimple(year: Int, month: Int)

    def top3Meses(natalityDF: DataFrame)(implicit sparkSession: SparkSession): Dataset[(Int, Long)] = {
      import sparkSession.implicits._

      val natalityDS: Dataset[NatalitySimple] = natalityDF
        .select("year", "month") // 💡 seleccionamos solo lo necesario
        .as[NatalitySimple]

      natalityDS.filter(_.year == 2005)
        .groupByKey(_.month)
        .count()
        .orderBy(desc(count))
        .limit(3)

    }

  }
}
