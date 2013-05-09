package edu.unc.mapseq.pipeline.ncgenes.doc;

import edu.unc.mapseq.pipeline.AbstractPipelineBeanService;

public class NCGenesDOCPipelineBeanService extends AbstractPipelineBeanService {

    private String referenceSequence;

    private String siteName;

    public NCGenesDOCPipelineBeanService() {
        super();
    }

    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    public String getReferenceSequence() {
        return referenceSequence;
    }

    public void setReferenceSequence(String referenceSequence) {
        this.referenceSequence = referenceSequence;
    }

}
