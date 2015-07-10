package gr.demokritos.iit.graphmethods;

import java.util.Vector;

/**
 *
 * @author JKLS
 */
public class Atom {
    
    //label of atom
    private String label;
    //actual atom
    private Object o1;
    //position of atom
    private Vector position;

    /**
     * Getter for label
     * @return label
     */
    public String getLabel() {
        return label;
    }

    /**
     * Getter for position
     * @return position
     */
    public Vector getPosition() {
        return position;
    }
    
}
