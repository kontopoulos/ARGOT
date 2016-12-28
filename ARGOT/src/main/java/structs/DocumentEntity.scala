package structs

import nlp.DocumentEntityTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import traits.{Atom, Entity}

/**
  * @author Kontopoulos Ioannis
  */
class DocumentEntity(val sc: SparkContext, slidingStep: Int, documentName: String) extends Entity {

  /**
    * Dissects the document into atoms based on
    * a function from the entity tokenizer
    * @param numPartitions
    * @return
    */
  override def getAtoms(numPartitions: Int): RDD[Atom] = {
    val partitionedDocument = sc.textFile(documentName,numPartitions)
    val tokenizer = new DocumentEntityTokenizer
    tokenizer.getCharacterNGrams(partitionedDocument,slidingStep)
  }

}
