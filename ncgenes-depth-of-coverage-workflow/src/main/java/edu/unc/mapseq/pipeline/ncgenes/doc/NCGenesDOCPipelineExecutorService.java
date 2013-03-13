package edu.unc.mapseq.pipeline.ncgenes.doc;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesDOCPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesDOCPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCGenesDOCPipelineBeanService pipelineBeanService;

    public void start() throws Exception {
        logger.info("ENTERING stop()");

        long delay = 15 * 1000; // 15 seconds
        long period = 5 * 60 * 1000; // 5 minutes

        NCGenesDOCPipelineExecutorTask task = new NCGenesDOCPipelineExecutorTask();
        task.setPipelineBeanService(pipelineBeanService);
        mainTimer.scheduleAtFixedRate(task, delay, period);

    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesDOCPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(NCGenesDOCPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
