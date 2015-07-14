package gr.demokritos.iit.graphmethods;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author JKLS
 */
public class StringFixedNGramSegmentor implements Segmentor {

    private final int ngram;

    public StringFixedNGramSegmentor(int ngram) {
        this.ngram = ngram;
    }

    /**
     * Segments a string entity into atoms
     *
     * @param e string entity
     * @return list of string atoms
     */
    @Override
    public List<StringAtom> getCompoments(Entity e) {
        StringEntity en = (StringEntity) e;
        List<StringAtom> atoms = new ArrayList();
        //begin index of string
        int begin = 0;
        //create substrings based on n-gram size
        for (int i = 1; i <= en.getDataString().length() - ngram + 1; i++) {
            //end index of string
            int end = begin + ngram;
            StringAtom a = new StringAtom();
            a.setPart("_" + en.getDataString().substring(begin, end));
            begin++;
            atoms.add(a);
        }
        return atoms;
    }

}
