package ru.spbu.apmath.st033672;

import java.util.List;

/**
 * Created by henry on 5/1/17.
 */
public class DocTFIDF {

    private String docName;

    private List<StringDouble> values;

    public DocTFIDF(String docName, List<StringDouble> values) {
        this.docName = docName;
        this.values = values;
    }
}
