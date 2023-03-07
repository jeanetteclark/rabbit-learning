import com.rabbitmq.client.*;
import java.io.*;

public class Worker {

  private static final String QUEUE_WORKER = "quality_queue";
  private static final String QUEUE_COMPLETED = "completed_queue";
  private static com.rabbitmq.client.Connection connection;
  private static com.rabbitmq.client.Channel channel;
  private static String final_message = "";

  public static void main(String[] argv) throws Exception {

    Worker wkr = new Worker();
    wkr.setupQueues();

    final Consumer consumer = new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            
            //Worker wkr = new Worker();
            
            String message = new String(body, "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            try {
                channel.basicAck(envelope.getDeliveryTag(), false);
                wkr.doWork(message);
            } catch (Exception e){
                System.out.println("Unable to send to Completed");
            }

            try {
                String message_final = "Completed:" + message;
                channel.basicPublish("", QUEUE_COMPLETED, MessageProperties.PERSISTENT_TEXT_PLAIN, message_final.getBytes("UTF-8"));
                // ack after success
                System.out.println(" [x]'" + message_final + "'");
            } catch (AlreadyClosedException rmqe){
                System.out.println("Unable to send to Completed");
                try {
                    wkr.setupQueues();
                    String message_final = "Completed: " + message;
                    channel.basicPublish("", QUEUE_COMPLETED, MessageProperties.PERSISTENT_TEXT_PLAIN, message_final.getBytes("UTF-8"));
                    channel.basicConsume(QUEUE_WORKER, false, this);
                    // ack after failure and successful retry
                    channel.basicAck(envelope.getDeliveryTag(), false);
                    System.out.println(" [x]" + message_final + "'");
                } catch (Exception e){
                    // nack and requeue after failures
                    channel.basicNack(envelope.getDeliveryTag(), false, true);
                    System.out.println("Unable to restart connection");
                }
            }
        }
  };
    channel.basicConsume(QUEUE_WORKER, false, consumer);
}

  private static void doWork(String task) {
    for (char ch : task.toCharArray()) {
        if (ch == '.') {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException _ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }
  }

  public void setupQueues () throws IOException {

    /* Connect to the RabbitMQ queue containing entries for which quality reports
       need to be created.
     */
    ConnectionFactory factory = new ConnectionFactory();
    boolean durable = true;
    try {
        connection = factory.newConnection();
        channel = connection.createChannel();

        channel.queueDeclare(QUEUE_WORKER, durable, false, false, null);
        // Channel will only send one request for each worker at a time.

        channel.basicQos(1);
        System.out.println("Connected to RabbitMQ queue " + QUEUE_WORKER);
        System.out.println("Waiting for messages. To exit press CTRL+C");
    } catch (Exception e) {
        System.out.println("exception");
    }

    try {
        channel.queueDeclare(QUEUE_COMPLETED, false, false, false, null);
        
    } catch (Exception e) {
        System.out.println("exception");
    }
}
}