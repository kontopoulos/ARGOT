package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
trait Entity {
  
  def getEntityComponents: List[Atom]
  
  def getPayload: Any
  
}