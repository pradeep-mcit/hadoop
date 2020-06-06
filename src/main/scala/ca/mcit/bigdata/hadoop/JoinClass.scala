package ca.mcit.bigdata.hadoop


case class RouteTrip
(
  trip: Trip, route: Option[Route]
)

case class RT_Calendar
(
  calendar: Calendar, routeTrip: Option[RouteTrip]
)