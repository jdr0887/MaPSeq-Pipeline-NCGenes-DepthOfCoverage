package edu.unc.mapseq.executor.ncgenes.doc;

import java.util.Date;
import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.WorkflowDAO;
import edu.unc.mapseq.dao.WorkflowRunAttemptDAO;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.workflow.WorkflowBeanService;
import edu.unc.mapseq.workflow.WorkflowExecutor;
import edu.unc.mapseq.workflow.WorkflowTPE;
import edu.unc.mapseq.workflow.ncgenes.doc.NCGenesDOCWorkflow;

public class NCGenesDOCWorkflowExecutorTask extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(NCGenesDOCWorkflowExecutorTask.class);

    private final WorkflowTPE threadPoolExecutor = new WorkflowTPE();

    private WorkflowBeanService workflowBeanService;

    public NCGenesDOCWorkflowExecutorTask() {
        super();
    }

    @Override
    public void run() {
        logger.info("ENTERING run()");

        threadPoolExecutor.setCorePoolSize(workflowBeanService.getCorePoolSize());
        threadPoolExecutor.setMaximumPoolSize(workflowBeanService.getMaxPoolSize());

        logger.info(String.format("ActiveCount: %d, TaskCount: %d, CompletedTaskCount: %d",
                threadPoolExecutor.getActiveCount(), threadPoolExecutor.getTaskCount(),
                threadPoolExecutor.getCompletedTaskCount()));

        WorkflowDAO workflowDAO = this.workflowBeanService.getMaPSeqDAOBean().getWorkflowDAO();
        WorkflowRunAttemptDAO workflowRunAttemptDAO = this.workflowBeanService.getMaPSeqDAOBean()
                .getWorkflowRunAttemptDAO();

        try {
            List<Workflow> workflowList = workflowDAO.findByName("NCGenesDOC");
            if (workflowList == null || (workflowList != null && workflowList.isEmpty())) {
                logger.error("No Workflow Found: {}", "NCGenesDOC");
                return;
            }
            Workflow workflow = workflowList.get(0);
            List<WorkflowRunAttempt> attempts = workflowRunAttemptDAO.findEnqueued(workflow.getId());
            if (attempts != null && !attempts.isEmpty()) {
                logger.info("dequeuing {} WorkflowRunAttempt", attempts.size());
                for (WorkflowRunAttempt attempt : attempts) {

                    NCGenesDOCWorkflow ncGenesDOCWorkflow = new NCGenesDOCWorkflow();
                    attempt.setVersion(ncGenesDOCWorkflow.getVersion());
                    attempt.setDequeued(new Date());
                    workflowRunAttemptDAO.save(attempt);

                    ncGenesDOCWorkflow.setWorkflowBeanService(workflowBeanService);
                    ncGenesDOCWorkflow.setWorkflowRunAttempt(attempt);
                    threadPoolExecutor.submit(new WorkflowExecutor(ncGenesDOCWorkflow));

                }

            }

        } catch (MaPSeqDAOException e) {
            e.printStackTrace();
        }

    }

    public WorkflowBeanService getWorkflowBeanService() {
        return workflowBeanService;
    }

    public void setWorkflowBeanService(WorkflowBeanService workflowBeanService) {
        this.workflowBeanService = workflowBeanService;
    }

}
