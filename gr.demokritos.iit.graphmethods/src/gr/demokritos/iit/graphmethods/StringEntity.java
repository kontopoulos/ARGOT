package gr.demokritos.iit.graphmethods;

import java.util.List;

/**
 *
 * @author JKLS
 */
public class StringEntity implements Entity {
    
    private String dataString;
    private List comps;

    public StringEntity(String dataString) {
        this.dataString = dataString;
    }

    public String getDataString() {
        return dataString;
    }

    public void setDataString(String dataString) {
        this.dataString = dataString;
    }

    public void setComps(List comps) {
        this.comps = comps;
    }

    /**
     * Gets the components of the string entity
     * @return list of string entity components
     */
    @Override
    public List<StringAtom> getComponents() {
        return comps;
    }

    @Override
    public Object getPayload() {
        //TODO code here, delete next line
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
