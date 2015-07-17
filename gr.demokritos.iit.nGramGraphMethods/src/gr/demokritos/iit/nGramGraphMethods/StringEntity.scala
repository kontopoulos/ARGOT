package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {
  
  var dataString = ""
  var components = null
  
  /**
   * @return components of String Entity
   */
  override def getEntityComponents: List[StringAtom] = components
  
  override def getPayload: Unit = println("Not supported yet.")
  
}