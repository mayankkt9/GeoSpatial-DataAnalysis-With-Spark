package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var rect_coordinates = new Array[String](4)
      rect_coordinates = queryRectangle.split(",")
      var rx1 = rect_coordinates(0).toDouble
      var ry1 = rect_coordinates(1).toDouble
      var rx2 = rect_coordinates(2).toDouble
      var ry2 = rect_coordinates(3).toDouble
      
      var pt_coordinates = new Array[String](2)
      pt_coordinates = pointString.split(",")
      var px = pt_coordinates(0).toDouble
      var py = pt_coordinates(1).toDouble

      if ((rx2 >= px && px >= rx1) || (rx2 <= px && px <= rx1)){
         if ((ry2 >= py && py >= ry1) || (ry2 <= py && py <= ry1)){
            return true
         }
      }
      return false
  }
}
