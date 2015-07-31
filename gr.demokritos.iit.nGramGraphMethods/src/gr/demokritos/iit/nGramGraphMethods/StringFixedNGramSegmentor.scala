package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class StringFixedNGramSegmentor(val ngram: Int) extends Segmentor {

  //create vertex ids based on characters
  val chars = Map(
    "a" -> "0",
    "b" -> "1",
    "c" -> "2",
    "d" -> "3",
    "e" -> "4",
    "f" -> "5",
    "g" -> "6",
    "h" -> "7",
    "i" -> "8",
    "j" -> "9",
    "k" -> "10",
    "l" -> "11",
    "m" -> "12",
    "n" -> "13",
    "o" -> "14",
    "p" -> "15",
    "q" -> "16",
    "r" -> "17",
    "s" -> "18",
    "t" -> "19",
    "u" -> "20",
    "v" -> "21",
    "w" -> "22",
    "x" -> "23",
    "y" -> "24",
    "z" -> "25",
    "A" -> "26",
    "B" -> "27",
    "C" -> "28",
    "D" -> "29",
    "E" -> "30",
    "F" -> "31",
    "G" -> "32",
    "H" -> "33",
    "I" -> "34",
    "J" -> "35",
    "K" -> "36",
    "L" -> "37",
    "M" -> "38",
    "N" -> "39",
    "O" -> "40",
    "P" -> "41",
    "Q" -> "42",
    "R" -> "43",
    "S" -> "44",
    "T" -> "45",
    "U" -> "46",
    "V" -> "47",
    "W" -> "48",
    "X" -> "49",
    "Y" -> "50",
    "Z" -> "51",
    "0" -> "52",
    "1" -> "53",
    "2" -> "54",
    "3" -> "55",
    "4" -> "56",
    "5" -> "57",
    "6" -> "58",
    "7" -> "59",
    "8" -> "60",
    "9" -> "61",
    "!" -> "62",
    "@" -> "63",
    "#" -> "64",
    "$" -> "65",
    "%" -> "66",
    "^" -> "67",
    "&" -> "68",
    "*" -> "69",
    "(" -> "70",
    ")" -> "71",
    "-" -> "72",
    "+" -> "73",
    "=" -> "74",
    "_" -> "75",
    "`" -> "76",
    "~" -> "77",
    "[" -> "78",
    "]" -> "79",
    "{" -> "80",
    "}" -> "81",
    ";" -> "82",
    "/" -> "83",
    "|" -> "84",
    "?" -> "85",
    "." -> "86",
    "," -> "87",
    ">" -> "88",
    "<" -> "89",
    " " -> "90",
    """'""" -> "91",
    '"' -> "92",
    """\""" -> "93",
    ":" -> "94"
  )

  /**
   * Segments a StringEntity into StringAtoms
   * @param e StringEntity
   * @return list of string atoms
   */
  override def getComponents(e: Entity): List[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    var atoms: List[StringAtom] = List()
    //begin index of string
    var begin = 0
    //create substrings based on ngram size
    for (i <- 1 to en.dataString.length() - ngram + 1) {
      //end index of string
      val end = begin + ngram
      val str = en.dataString.substring(begin, end)
      var label = ""
      //match each character with its corresponding unicode number
      str.foreach{ c => label = label + c.toString.foldLeft(1L)(_ * _.toInt) }
      atoms :::= List(new StringAtom(label, "_" + str))
      begin += 1
    }
    atoms.reverse
  }
  
}