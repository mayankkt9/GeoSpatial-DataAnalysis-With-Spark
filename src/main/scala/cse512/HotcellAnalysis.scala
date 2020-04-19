package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  val nCells = numCells.toDouble

  pickupInfo.createOrReplaceTempView("pickupInfo")

  val freqSQL = "select x, y, z, count(*) as frequency from pickupInfo where x between %f and %f and y between %f and %f and z between %d and %d group by x, y, z"
  
  val pickupPointsCountsDF = spark.sql(freqSQL.format(minX, maxX, minY, maxY, minZ, maxZ))
 
  pickupPointsCountsDF.createOrReplaceTempView("pickupPointsCounts")

  // Calculate sigma Xi and sigma X square i
  val totalFreqDF = spark.sql("select sum(frequency), sum(frequency*frequency) from pickupPointsCounts")

  // Value of X bar
  val X = totalFreqDF.first().getLong(0).toDouble/nCells

  // Value of S
  val S = Math.sqrt(totalFreqDF.first().getLong(1).toDouble/nCells - X*X)

  spark.udf.register("findNeighbors", (maxX: Double, minX: Double, maxY: Double, minY: Double, maxZ: Int, minZ: Int, X: Double, Y: Double, Z: Int)=>((
    HotcellUtils.findNeighbors(maxX, minX, maxY, minY, maxZ, minZ, X, Y, Z)
    )))

  val WiXi_SQL = "select findNeighbors( %f, %f, %f, %f, %d, %d, a.x, a.y, a.z) as Wij, sum(b.frequency) as Xj, a.x as X, a.y as Y, a.z as Z from pickupPointsCounts a, pickupPointsCounts b where" +
                " (b.x = a.x or b.x = a.x + 1 or b.x = a.x - 1) and (b.y = a.y or b.y = a.y + 1 or b.y = a.y - 1) and (b.z = a.z or b.z = a.z + 1 or b.z = a.z - 1) group by a.x,a.y,a.z" 

  // Calculate Wij and Xi
  val WiXiDF = spark.sql(WiXi_SQL.format(maxX, minX, maxY, minY, maxZ, minZ))

  WiXiDF.createOrReplaceTempView("WiXiValues")

  spark.udf.register("calculateZ", (w: Double, sumX: Double, s: Double, nCells: Double, xBar: Double)=>((
    HotcellUtils.calculateZ(w, sumX, s, nCells, xBar)
    )))

  val zScoreSQL = "select x, y, z, calculateZ(Wij, Xj, %f, %f, %f) as ZScore from WiXiValues"

  // Calculte GScore
  val zScoreDF = spark.sql(zScoreSQL.format(S, nCells, X))

  zScoreDF.createOrReplaceTempView("zScore")

  val resultSet = spark.sql("select x, y, z from zScore order by ZScore DESC limit 50")

  return resultSet
}

}
