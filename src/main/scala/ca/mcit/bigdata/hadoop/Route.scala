package ca.mcit.bigdata.hadoop

case class Route(
                  route_id: Int,
                  agency_id: String,
                  route_short_name: String,
                  route_long_name: String,
                  route_type: String,
                  route_url: String,
                  route_color: String,
                  route_text_color: String
                )

object Route {
  def apply(csvLine: String): Route = {
    val s = csvLine.split(",", -1)
    new Route(s(0).toInt, s(1), s(2), s(3), s(4), s(5), s(6), s(7))
  }

  def toCsv(route: Route): String = {
    route.route_id + "," +
      route.agency_id + "," +
      route.route_short_name + "," +
      route.route_long_name + "," +
      route.route_url + "," +
      route.route_color + "," +
      route.route_text_color
  }
}