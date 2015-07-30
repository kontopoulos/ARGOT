package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {
  
  var dataString = ""
  private var components: List[StringAtom] = List()
  
  /**
   * @return components of String Entity
   */
  override def getEntityComponents: List[StringAtom] = components

  def setEntityComponents(list: List[StringAtom]): Unit = components = list

  def readDataStringFromFile(file: String): Unit = {
    try {
      val source = scala.io.Source.fromFile(file)
      dataString = source.mkString
      source.close()
    }
    catch {
      case ex: Exception => println(ex.getMessage)
    }
  }
  
  override def getPayload: Unit = println("Not supported yet.")
  
}