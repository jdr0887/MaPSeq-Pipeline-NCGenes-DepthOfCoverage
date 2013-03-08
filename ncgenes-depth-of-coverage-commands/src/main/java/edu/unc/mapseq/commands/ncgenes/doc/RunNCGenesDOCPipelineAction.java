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
import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.karaf.shell.console.AbstractAction;

import edu.unc.mapseq.config.MaPSeqConfigurationService;
import edu.unc.mapseq.dao.MaPSeqDAOBean;
import edu.unc.mapseq.dao.model.HTSFSample;

@Command(scope = "mapseq", name = "run-ncgenes-doc-pipeline", description = "Run NCGenes DepthOfCoverage")
public class RunNCGenesDOCPipelineAction extends AbstractAction {

    @Argument(index = 0, name = "htsfSampleId", description = "HTFSSample Identifier", required = true, multiValued = false)
    private Long htsfSampleId;

    @Argument(index = 1, name = "workflowRunName", description = "WorkflowRun.name", required = true, multiValued = false)
    private String workflowRunName;

    @Argument(index = 2, name = "prefix", description = "prefix", required = true, multiValued = false)
    private String prefix;

    @Argument(index = 3, name = "summaryCoverageThreshold", description = "Summary Coverage Threshold", required = true, multiValued = false)
    private String summaryCoverageThreshold;

    @Argument(index = 4, name = "intervalList", description = "Interval List file", required = false, multiValued = false)
    private String intervalList;

    private MaPSeqDAOBean mapseqDAOBean;

    private MaPSeqConfigurationService mapseqConfigurationService;

    public RunNCGenesDOCPipelineAction() {
        super();
    }

    @Override
    public Object doExecute() {

        HTSFSample sample = null;
        try {
            sample = mapseqDAOBean.getHTSFSampleDAO().findById(htsfSampleId);
        } catch (Exception e1) {
        }

        if (sample == null) {
            System.err.println("HTSFSample not found: ");
            return null;
        }

        if (StringUtils.isNotEmpty(intervalList)) {
            File intervalListFile = new File(intervalList);
            if (!intervalListFile.exists()) {
                System.out.printf("Interval List file does not exist...check path: %s",
                        intervalListFile.getAbsolutePath());
                return null;
            }
        }

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                mapseqConfigurationService.getWebServiceHost("localhost")));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.doc");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            if (StringUtils.isNotEmpty(intervalList)) {
                String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"HTSFSample\",\"guid\":\"%d\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.prefix\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.summaryCoverageThreshold\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.intervalList\",\"value\":\"%s\"}]},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s\"}]}";
                producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"),
                        htsfSampleId, prefix, summaryCoverageThreshold, intervalList, workflowRunName)));
            } else {
                String format = "{\"account_name\":\"%s\",\"entities\":[{\"entity_type\":\"HTSFSample\",\"guid\":\"%d\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.prefix\",\"value\":\"%s\"},{\"name\":\"GATKDepthOfCoverage.summaryCoverageThreshold\",\"value\":\"%s\"}]},{\"entity_type\":\"WorkflowRun\",\"name\":\"%s\"}]}";
                producer.send(session.createTextMessage(String.format(format, System.getProperty("user.name"),
                        htsfSampleId, prefix, summaryCoverageThreshold, workflowRunName)));
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

    public Long getHtsfSampleId() {
        return htsfSampleId;
    }

    public void setHtsfSampleId(Long htsfSampleId) {
        this.htsfSampleId = htsfSampleId;
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

    public MaPSeqDAOBean getMapseqDAOBean() {
        return mapseqDAOBean;
    }

    public void setMapseqDAOBean(MaPSeqDAOBean mapseqDAOBean) {
        this.mapseqDAOBean = mapseqDAOBean;
    }

    public MaPSeqConfigurationService getMapseqConfigurationService() {
        return mapseqConfigurationService;
    }

    public void setMapseqConfigurationService(MaPSeqConfigurationService mapseqConfigurationService) {
        this.mapseqConfigurationService = mapseqConfigurationService;
    }

    public String getIntervalList() {
        return intervalList;
    }

    public void setIntervalList(String intervalList) {
        this.intervalList = intervalList;
    }

}
