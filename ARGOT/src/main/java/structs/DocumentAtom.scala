package structs

import traits.Atom

/**
 * @author Kontopoulos Ioannis
 */
case class DocumentAtom(val label: Long, val dataStream: String) extends Atom
