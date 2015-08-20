package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {

  //actual string of the entity
  var dataString = ""
  //list of components of StringEntity
  private var components: List[StringAtom] = List()
  
  /**
   * @return components of String Entity
   */
  override def getEntityComponents: List[StringAtom] = components

  /**
   * Sets the list of components of the Entity
   * @param list list of components
   */
  def setEntityComponents(list: List[StringAtom]): Unit = components = list

  /**
   * Reads dataString from a file
   * @param file file to be read
   */
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