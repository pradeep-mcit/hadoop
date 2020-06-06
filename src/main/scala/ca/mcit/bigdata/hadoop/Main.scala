package ca.mcit.bigdata.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}


trait  Main  {

  val conf = new Configuration()

  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop/etc/cloudera/hdfs-site.xml"))

  //whenever we need to override specific key
  // conf.set("fs.defaultFS","hdfs://quickstart.cloudera:8020")
  //println(conf.get("fs.defaultFS"))
//println(FileSystem.get(conf).getClass.getName)

  val fs = FileSystem.get(conf)

  //val fileList = fs.listStatus(new Path("/user/summer2019"))



   /*val routeFile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/movie.csv"),new Path("/user/summer2019/pradeep"))
  val tripFile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/rating.csv"),new Path("/user/summer2019/pradeep"))
  val tripFiile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/reviewer.csv"),new Path("/user/summer2019/pradeep")) */

  val calFiile = fs.copyFromLocalFile(new Path("/home/bd-user/Documents/calendar_dates.txt"),new Path("/user/summer2019/pradeep/project4"))
  val freqFiile = fs.copyFromLocalFile(new Path("/home/bd-user/Downloads/frequencies.txt"),new Path("/user/summer2019/pradeep/project4"))

  //val source = fs.open( new Path("/user/summer2019/pradeep/routes.txt"))
  //("/home/bd-user/Documents/routes.txt","/user/summer2019/pradeep")

  //"/home/bd-user/Documents/routes.txt" "/user/summer2019"



 // println(routeFile)
  //println(tripFile)



    }


