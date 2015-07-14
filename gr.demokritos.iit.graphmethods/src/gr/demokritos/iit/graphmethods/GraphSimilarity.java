package gr.demokritos.iit.graphmethods;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author JKLS
 */
public class GraphSimilarity implements Similarity {

    //value of size similarity
    private double sizeSimilarity;
    //value of value similarity
    private double valueSimilarity;
    //value of contaiment similarity
    private double containmentSimilarity;
    //map containing all the similarities
    private final Map components;

    /**
     * Constructor
     */
    public GraphSimilarity() {
        components = new HashMap();
    }
    
    /**
     * Calculates the overall similarity
     * @return overall similarity
     */
    @Override
    public double getOverallSimilarity() {
        return sizeSimilarity * valueSimilarity * containmentSimilarity;
    }

    /**
     * 
     * @return map of graph similarity components
     */
    @Override
    public Map getSimilarityComponents() {
        components.put("size", sizeSimilarity);
        components.put("value", valueSimilarity);
        components.put("containment", containmentSimilarity);
        return components;
    }

    /**
     * Setter for size similarity
     * @param sizeSimilarity 
     */
    public void setSizeSimilarity(double sizeSimilarity) {
        this.sizeSimilarity = sizeSimilarity;
    }

    /**
     * Setter for value similarity
     * @param valueSimilarity 
     */
    public void setValueSimilarity(double valueSimilarity) {
        this.valueSimilarity = valueSimilarity;
    }

    /**
     * Setter for containment similarity
     * @param containmentSimilarity 
     */
    public void setContainmentSimilarity(double containmentSimilarity) {
        this.containmentSimilarity = containmentSimilarity;
    }  
    
}
