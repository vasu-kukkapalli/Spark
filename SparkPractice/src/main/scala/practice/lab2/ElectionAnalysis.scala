package practice.lab2
import scala.io.Source

object ElectionAnalysis {
  def main(args: Array[String]): Unit = {
    
    val fileName = "/home/vara/Vasu/Spark/Practice/input/elections.tsv"
        /*
state	constituency	candidate_name	sex	age	category	partyname	partysymbole	general	posatal	total	pct_of_total_votes	pct_of_polled_votes	totalvoters
Andhra Pradesh	Adilabad 	GODAM NAGESH	M	49	ST	TRS	Car	425762	5085	430847	31.07931864	40.81807244	1386282
Andhra Pradesh	Adilabad 	NARESH	M	37	ST	INC	Hand	257994	1563	259557	18.72324679	24.59020587	1386282 */
    
    val data = Source.fromFile(fileName).getLines()//.take(5).foreach { println }
   /* val fil = data.filter { x => x.split("\t")(2) == "None of the Above" }
    val tup = fil.map(rec => (rec.split("\t")(0),rec.split("\t")(10).toInt)).
    toList.
    groupBy(rec => rec._1).
    map(rec => (rec._1, rec._2.map(_._2).reduce(_+_))).
    toList.
    sortBy(rec => -rec._2).
    foreach(rec => println(rec._1 + "\t" + rec._2))*/
    
    //Get all the distinct constituencies
    
    val distConstitus = data.map { x => x.split("\t")(1) }.toList.distinct.sorted.foreach(println)
    
  }
}