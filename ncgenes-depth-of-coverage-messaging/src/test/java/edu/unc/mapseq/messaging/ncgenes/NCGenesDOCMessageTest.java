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
            // String value =
            // "{\"account_name\":\"powen\",\"entities\":[{\"entity_type\":\"HTSF Sample\",\"guid\":\"27354\",\"attributes\":[{\"name\":\"GATKDepthOfCoverage.interval_list.version\",\"value\":\"3\"},{\"name\":\"SAMToolsView.dx.id\",\"value\":\"2\"}]},{\"entity_type\":\"Workflow run\",\"name\":\"NCG_00007_Analysis_Pipeline_Run\"}]}";
            // producer.send(session.createTextMessage(value));

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
