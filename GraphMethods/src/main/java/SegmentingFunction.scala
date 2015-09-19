/**
 * @author Kontopoulos Ioannis
 */
trait SegmentingFunction {

  def getComponents(e: Entity): List[Atom]

}