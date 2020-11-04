import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, regexp_extract, when}
import org.apache.spark.sql.{Column, SparkSession, functions}




object Pruebas extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("WebLog").master("local[*]").getOrCreate()
  import spark.implicits._
  //this will produce a dataframe with a single column called value
  val base_df = spark.read.csv("weblog.csv")
  base_df.printSchema()

  //let's look at some of the data
  base_df.show(3,false)

  /* So our data is organized as follows
  |remote host  : 10.128.2.1
  | date        : [29/Nov/2017:06:58:55
  | request     : GET /login.php HTTP/1.1
  | status      : 200
   */



  val parsed_df = base_df.select(($"_c0").alias("host"),
    ($"_c1").as("timestamp"),
    ($"_c2").as("path"),
    ($"_c3").cast("int").alias("status"))
  parsed_df.show(5,false )
  parsed_df.printSchema()


  println("total de lineas sin  quitar nulos:" +parsed_df.count())

  println("total de lineas quitando nulos: " + parsed_df.na.drop.count())
  val cleaned_df=parsed_df.na.drop()
  println("total de lineas sin  quitar nulos:" +cleaned_df.count())



 parsed_df.describe("path").show()
  val contadorHost=parsed_df.filter($"host" like  "10.128%").count()
  println("El numero de host que empiezan por 10.128 es: " + contadorHost)

 //parsed_df.groupBy("status").max("status").show()
  val contador= cleaned_df.groupBy("status").count()
  contador.show()
  contador.select(functions.max(col("count"))).show()
  //cleaned_df.select(max("status")).show()

  val estados = spark.read.text("estados.txt")
  estados.printSchema()

  //let's look at some of the data
  estados.show(3,false)

  val cadena2 ="""^(\d+)-(.*)$"""
  val estadosParseados = estados.select(regexp_extract($"value",cadena2,1).alias("status"),
    regexp_extract($"value",cadena2,2).as("descripcion"))
  estadosParseados.show(5,false )
  estadosParseados.printSchema()

  //UNIR TABLAS JOIN
  estadosParseados.join(parsed_df,"status").select("status","descripcion","path").show()

  //def añadirDesc(x: String): String= Columna.alias(s"Descripcion:${Columna.toString()}")
  //estadosParseados.select(añadirDesc(col("descripcion"))).show()

  //def contarVeces(Columna: Dataset[Row]): Dataset[Row] =  cleaned_df.groupBy("Columna").count().select(functions.max(col("count"))).alias(s"max_${Columna.toString()}")
 // contarVeces(cleaned_df).show()

  def actualizar(Columna: Column): Column= when($"status".equalTo(200),"CORRECTO").otherwise(Columna).alias(Columna.toString())
  //val actualizar= estadosParseados.withColumn("descripcion", when($"status".equalTo(200),"CORRECTO").otherwise($"descripcion"))
  estadosParseados.select(actualizar(col("descripcion"))).show()





}
