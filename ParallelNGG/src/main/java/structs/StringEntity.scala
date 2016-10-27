/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {

  // data string
  private var dataString = ""

  override def getPayload = dataString

  def fromFile(f: String): Unit = {
    dataString = scala.io.Source.fromFile(f).mkString
  }

  def setDataString(data: String): Unit = {
    dataString = data
  }

}
