package ca.mcit.bigdata.hadoop

case class Calendar(service_id: String,
                    monday: Int,
                    tuesday: Int,
                    wednesday: Int,
                    thursday: Int,
                    friday: Int,
                    saturday: Int,
                    sunday: Int,
                    start_date: String,
                    end_date: String
                   )

object Calendar {

  def apply(csvLine: String): Calendar = {
    val p = csvLine.split(",", -1)
    new Calendar(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7).toInt, p(8), p(9))
  }

  def toCsv(calender: Calendar): String = {
    calender.service_id + "," +
      calender.monday + "," +
      calender.tuesday + "," +
      calender.wednesday + "," +
      calender.thursday + "," +
      calender.friday + "," +
      calender.saturday + "," +
      calender.sunday + "," +
      calender.start_date + "," +
      calender.end_date
  }
}