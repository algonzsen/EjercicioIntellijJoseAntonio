import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}

object Pruebas3 extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("dias").master("local[*]").getOrCreate()
  val ventasDias=Seq(("lunes", 100), ("martes", 150), ("miércoles", 140), ("jueves", 145), ("viernes", 155), ("sabado", 90), ("domingo", 85))
  val miRDD= spark.sparkContext.parallelize(ventasDias)
  val tablaVentas=spark.createDataFrame(miRDD).toDF("dia","ventas")
  tablaVentas.show()

  def Funcion (x: String): String ={
    if (
      (x.equals("sabado")) || (x.equals("domingo")) )
      "festivo" else "laboral"
  }



  val udfTipoDia = udf[String,String](Funcion(_))
  val ventasTipoDia=tablaVentas.withColumn("TipoDia", udfTipoDia(col("dia")))
  ventasTipoDia.show

  val numbers= List(0,1,2,3,4,5,6,7,8)
  val numbersRDD= spark.sparkContext.parallelize(numbers)
  numbersRDD.map(x=>x+2).foreach(println(_))


}
