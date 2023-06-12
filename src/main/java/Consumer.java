import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


public class Consumer {
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
            thread(new Subscriber(prop), false);
        }
    }

    public static void thread(Runnable runnable, boolean daemon) {
        Thread brokerThread = new Thread(runnable);
        brokerThread.setDaemon(daemon);
        brokerThread.start();
    }

    public static class Subscriber implements Runnable, ExceptionListener {
        private String hostname = null;
        private String port = null;
        private String queueName = null;
        private String timeout = null;
        long messageNumberPerThread = 0L;
        boolean consoleOutput;
        public Subscriber(Properties prop) {
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
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("failover://(tcp://" + hostname + ":" + port + ")?timeout=" + timeout + "&initialReconnectDelay=2000&maxReconnectAttempts=2");
                Connection connection = connectionFactory.createConnection();
                connection.start();
                Session session = connection.createSession(Boolean.TRUE, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue(queueName);
                MessageConsumer consumer = session.createConsumer(destination);

                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS");
                startTime = dtf.format(LocalDateTime.now());
                TextMessage message = null;
                while ((message = (TextMessage) consumer.receiveNoWait()) != null) {
                    if(consoleOutput) {
                        System.out.println("Receive: " + message.getText());
                    }
                    session.commit();
                    messageCount++;
                }
                session.close();
                connection.close();
                endTime = dtf.format(LocalDateTime.now());
                System.out.println(Thread.currentThread().getName() +  ", " + startTime +  "-" + endTime + ", messageCount: " + messageCount);
                consumer.close();
                session.close();
                connection.close();
            } catch (Exception e) {
                System.out.println(Thread.currentThread().getName() +  ", " + startTime +  "-" + endTime + ", messageCount: " + messageCount);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occured.  Shutting down client.");
        }
    }
}