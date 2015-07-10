package gr.demokritos.iit.graphmethods;

import java.util.List;

/**
 *
 * @author JKLS
 */
public interface Entity {
    
    /**
     * Gets the components of the entity
     * @return list of entity components
     */
    public List getComponents();
    
    public Object getPayload();
    
}
