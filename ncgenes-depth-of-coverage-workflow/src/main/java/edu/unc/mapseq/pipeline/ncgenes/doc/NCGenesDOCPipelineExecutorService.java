package edu.unc.mapseq.pipeline.ncgenes.doc;

import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NCGenesDOCPipelineExecutorService {

    private final Logger logger = LoggerFactory.getLogger(NCGenesDOCPipelineExecutorService.class);

    private final Timer mainTimer = new Timer();

    private NCGenesDOCPipelineExecutorTask task;

    private Long period = 5L;

    public NCGenesDOCPipelineExecutorService() {
        super();
    }

    public void start() throws Exception {
        logger.info("ENTERING start()");
        long delay = 1 * 60 * 1000; // 1 minute
        mainTimer.scheduleAtFixedRate(task, delay, period * 60 * 1000);
    }

    public void stop() throws Exception {
        logger.info("ENTERING stop()");
        mainTimer.purge();
        mainTimer.cancel();
    }

    public NCGenesDOCPipelineExecutorTask getTask() {
        return task;
    }

    public void setTask(NCGenesDOCPipelineExecutorTask task) {
        this.task = task;
    }

    public Long getPeriod() {
        return period;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

}
