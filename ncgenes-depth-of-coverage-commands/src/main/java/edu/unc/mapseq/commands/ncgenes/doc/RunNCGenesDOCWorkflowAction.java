package edu.unc.mapseq.commands.ncgenes.doc;

import java.io.File;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.karaf.shell.api.action.Action;
import org.apache.karaf.shell.api.action.Argument;
import org.apache.karaf.shell.api.action.Command;
import org.apache.karaf.shell.api.action.lifecycle.Reference;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBeanService;
import edu.unc.mapseq.dao.model.Sample;

@Command(scope = "ncgenes-doc", name = "run-workflow", description = "Run NCGenes DepthOfCoverage Workflow")
public class RunNCGenesDOCWorkflowAction implements Action {

    @Argument(index = 0, name = "sampleId", description = "Sample Identifier", required = true, multiValued = false)
    private Long sampleId;

    @Argument(index = 1, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    @Argument(index = 2, name = "prefix", description = "prefix", required = true, multiValued = false)
    private String prefix;

    @Argument(index = 3, name = "summaryCoverageThreshold", description = "Summary Coverage Threshold", required = true, multiValued = false)
    private String summaryCoverageThreshold;

    @Argument(index = 4, name = "intervalList", description = "Interval List file", required = false, multiValued = false)
    private String intervalList;

    @Reference
    private MaPSeqDAOBeanService maPSeqDAOBeanService;

    @Reference
    private MaPSeqConfigurationService maPSeqConfigurationService;

    public RunNCGenesDOCWorkflowAction() {
        super();
    }

    @Override
    public Object execute() {

        Sample sample = null;
        try {
            sample = maPSeqDAOBeanService.getSampleDAO().findById(sampleId);
        } catch (Exception e1) {
        }

        if (sample == null) {
            System.err.println("Sample not found: ");
            return null;
        }

        if (StringUtils.isNotEmpty(intervalList)) {
            File intervalListFile = new File(intervalList);
            if (!intervalListFile.exists()) {
                System.out.printf("Interval List file does not exist...check path: %s", intervalListFile.getAbsolutePath());
                return null;
            }
        }

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                String.format("nio://%s:61616", maPSeqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.doc");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            if (StringUtils.isNotEmpty(intervalList)) {
                String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.prefix\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.summaryCoverageThreshold\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.intervalList\",\"value\":\"%s\"}]},{\"entityType\":\"WorkflowRun\",\"name\":\"%s\"}]}";
                producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"), sampleId, prefix,
                        summaryCoverageThreshold, intervalList, workflowRunName)));
            } else {
                String format = "{\"entities\":[{\"entityType\":\"Sample\",\"id\":\"%d\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.prefix\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.summaryCoverageThreshold\",\"value\":\"%s\"}]},{\"entityType\":\"WorkflowRun\",\"name\":\"%s\"}]}";
                producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"), sampleId, prefix,
                        summaryCoverageThreshold, workflowRunName)));
            }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        return null;
    }

    public Long getSampleId() {
        return sampleId;
    }

    public void setSampleId(Long sampleId) {
        this.sampleId = sampleId;
    }

    public String getWorkflowRunName() {
        return workflowRunName;
    }

    public void setWorkflowRunName(String workflowRunName) {
        this.workflowRunName = workflowRunName;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public String getSummaryCoverageThreshold() {
        return summaryCoverageThreshold;
    }

    public void setSummaryCoverageThreshold(String summaryCoverageThreshold) {
        this.summaryCoverageThreshold = summaryCoverageThreshold;
    }

    public String getIntervalList() {
        return intervalList;
    }

    public void setIntervalList(String intervalList) {
        this.intervalList = intervalList;
    }

}
