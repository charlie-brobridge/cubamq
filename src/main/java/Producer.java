import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class Producer {
    public static void main(String[] args) throws Exception {
        System.out.println(args[0]);
        Properties prop = new Properties();
        long threadNumber = 0L;
        try (InputStream input = new FileInputStream(args[0])) {
            prop.load(input);
            threadNumber = Long.parseLong(prop.getProperty("thread.number"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        for (int i = 0; i < threadNumber; i++) {
            thread(new Publisher(prop), false);
        }
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class Publisher implements Runnable {
        private String hostname = null;
        private String port = null;
        private String queueName = null;
        private String timeout = null;
        long messageNumberPerThread = 0L;
        boolean consoleOutput;
        public Publisher(Properties prop) {
            hostname = prop.getProperty("amq.broker.hostname");
            port = prop.getProperty("amq.broker.port");
            queueName = prop.getProperty("amq.queue.name");
            timeout = prop.getProperty("amq.timeout");
            messageNumberPerThread = Long.parseLong(prop.getProperty("message.number.per.thread"));
            consoleOutput = Boolean.parseBoolean(prop.getProperty("console.output"));
        }
        public void run() {
            String startTime = null;
            String endTime = null;
            long messageCount = 0L;
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover://(tcp://" + hostname + ":" + port + ")?timeout=" + timeout +"&initialReconnectDelay=2000&maxReconnectAttempts=2");
                Connection connection = connectionFactory.createConnection();
                connection.start();
                Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(queueName);
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

                // Create a messages
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
                startTime = dtf.format(LocalDateTime.now());
                for(int i = 0; i < messageNumberPerThread; i++) {
                    TextMessage textMessage = session.createTextMessage(Thread.currentThread().getName() + " : " + String.valueOf(i+1));
                    if(consoleOutput) {
                        System.out.println("Send: " + textMessage.getText());
                    }
                    producer.send(textMessage);
                    session.commit();
                    messageCount++;
                }
                session.close();
                connection.close();
                endTime = dtf.format(LocalDateTime.now());
                System.out.println(Thread.currentThread().getName() +  ", " + startTime +  "-" + endTime + ", messageCount: " + messageCount);
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() +  ", " + startTime +  "-" + endTime + ", messageCount: " + messageCount);
                e.printStackTrace();
            }
        }
    }
}