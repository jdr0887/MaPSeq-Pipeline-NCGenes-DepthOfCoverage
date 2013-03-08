package edu.unc.mapseq.pipeline.ncgenes.doc;

import edu.unc.mapseq.pipeline.AbstractPipelineBeanService;

public class NCGenesDOCPipelineBeanService extends AbstractPipelineBeanService {

    private String referenceSequence;

    public NCGenesDOCPipelineBeanService() {
        super();
    }

    public String getReferenceSequence() {
        return referenceSequence;
    }

    public void setReferenceSequence(String referenceSequence) {
        this.referenceSequence = referenceSequence;
    }

}
