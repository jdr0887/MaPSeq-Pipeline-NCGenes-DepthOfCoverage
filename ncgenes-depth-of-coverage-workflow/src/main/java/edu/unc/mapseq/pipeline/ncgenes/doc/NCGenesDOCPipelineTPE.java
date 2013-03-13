package edu.unc.mapseq.pipeline.ncgenes.doc;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NCGenesDOCPipelineTPE extends ThreadPoolExecutor {

    public NCGenesDOCPipelineTPE() {
        super(10, 10, 5L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    }

}
