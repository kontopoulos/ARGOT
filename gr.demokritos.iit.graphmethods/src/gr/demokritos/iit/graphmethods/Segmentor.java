package gr.demokritos.iit.graphmethods;

import java.util.List;

/**
 *
 * @author JKLS
 */
public interface Segmentor {
    
    /**
     * Segments an entity into components
     * @param e entity
     * @return list of entity components
     */
    public List getCompoments(Entity e);
    
}
