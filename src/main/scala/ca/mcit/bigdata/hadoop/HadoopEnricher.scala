package ca.mcit.bigdata.hadoop

import org.apache.hadoop.fs.Path

object HadoopEnricher extends Main with App {

  val filelist = fs.listStatus(new Path("/user/summer2019"))


  val tripStream = fs.open(new Path("/user/summer2019/pradeep/stm/trips.txt"))
  val tripList: List[Trip] = Iterator.continually(tripStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(s => Trip(s(0).toInt, s(1), s(2), s(3), s(4).toInt, s(5).toInt, s(6).toInt,
      if (s(7).isEmpty) None else Some(s(7)),
      if (s(8).isEmpty) None else Some(s(8))))
val x= Trip.toCsv(tripList.asInstanceOf[Trip])

  val routeStream = fs.open(new Path("/user/summer2019/pradeep/stm/routes.txt"))
  val routeList: List[Route] = Iterator.continually(routeStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Route(p(0).toInt, p(1), p(2), p(3), p(4), p(5), p(6), p(7)))


  val routeTrip: List[JoinOutput] = new GenericMapJoin[Trip, Route]((i) => i.route_id.toString)((j) => j.route_id.toString)
    .join(tripList, routeList)


  val calendarStream = fs.open(new Path("/user/summer2019/pradeep/stm/calendar.txt"))
  val calendarList: List[Calendar] = Iterator.continually(calendarStream.readLine()).takeWhile(_ != null)
    .toList
    .tail
    .map(_.split(",", -1))
    .map(p => Calendar(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7).toInt, p(8), p(9)))



  val enrichedTrip = new GenericNestedLoopJoin[Calendar, JoinOutput]((i, j) => i.service_id == j.left.asInstanceOf[Trip].service_id)
    .join(calendarList, routeTrip)


  val output: List[String] = enrichedTrip
    .map(joinOutput => {

      val y = Trip.toCsv(joinOutput.right.getOrElse("").asInstanceOf[JoinOutput].left.asInstanceOf[Trip])
      val r = Route.toCsv(joinOutput.right.getOrElse("").asInstanceOf[JoinOutput].right.getOrElse("").asInstanceOf[Route])
      val c = Calendar.toCsv(joinOutput.left.asInstanceOf[Calendar])
      y + "," + r + "," + c
    })

// c = Calendar.toCsvlendar]
  val outPath = new Path("/user/summer2019/pradeep/course3/rt_calendar.txt")

  if (fs.exists(outPath)) {
    fs.delete(outPath)
    fs.mkdirs(new Path("/user/summer2019/pradeep/course3"))
  }
    fs.createNewFile(new Path("/user/summer2019/pradeep/course3/rt_calendar.txt"))
    val fileHeader: String = "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color,service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date"


    val outputStream = fs.append(outPath)
    outputStream.writeChars(fileHeader + "\n")
    output.foreach(enrichedTrip => outputStream.writeChars(enrichedTrip + "\n"))
    outputStream.close()
//  }
//  else {
//
//    fs.createNewFile(new Path("/user/summer2019/pradeep/course3/rt_calendar.txt"))
//    val fileHeader: String = "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en,route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color,service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date"
//
//    val outputStream = fs.append(outPath)
//    outputStream.writeChars(fileHeader + "\n")
//    output.foreach(enrichedTrip => outputStream.writeChars(enrichedTrip + "\n"))
    outputStream.close()
  //}
}


