package edu.unc.mapseq.messaging.ncgenes;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.Test;

public class NCGenesDOCMessageTest {

    @Test
    public void testQueue() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(String.format("nio://%s:61616",
                "biodev2.its.unc.edu"));

        Connection connection = null;
        Session session = null;
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createQueue("queue/ncgenes.doc");
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            String format = "{\"account_name\": \"%s\",\"entities\": [{\"attributes\": [{\"name\": \"GATKDepthOfCoverage.intervalList\",\"value\": \"%s\"},{\"name\": \"GATKDepthOfCoverage.prefix\",\"value\": \"%s\"},{\"name\": \"GATKDepthOfCoverage.summaryCoverageThreshold\",\"value\": \"%s\"}],\"entity_type\": \"HTSFSample\",\"guid\": \"%d\"},{\"entity_type\": \"WorkflowRun\",\"name\": \"%s\"}]}";
            producer.send(session.createTextMessage(String.format(format, "rc_renci.svc",
                    "/proj/renci/sequence_analysis/annotation/abeast/NCGenes/13/exons_pm_0_v13.interval_list",
                    "NCG_00009.52422", "1,2,5,8,10,15,20,30,50", 52422L, "UNIQUEJOBNAME")));

            // NCG_00009.52422.DOC.ebb2f3fe-a11b-11e2-9390-001ec9393b7a
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

    }
}
