import com.rabbitmq.client.*;
import java.io.*;

public class Worker {

  private static final String TASK_QUEUE_NAME = "task_queue";
  private static final String COMPLETED_QUEUE = "completed_queue";
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
                wkr.doWork(message);
            } catch (Exception e){
                System.out.println("Unable to send to Completed");
            }

            try {
                String message_final = "Pass it along";
                channel.basicPublish("", COMPLETED_QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message_final.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message_final + "'");
            } catch (Exception e){
                System.out.println("Unable to send to Completed");
            } finally {
                System.out.println(" [x] Done");
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        }
  };
    channel.basicConsume(TASK_QUEUE_NAME, false, consumer);
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

        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
        // Channel will only send one request for each worker at a time.

        channel.basicQos(1);
        System.out.println("Connected to RabbitMQ queue " + TASK_QUEUE_NAME);
        System.out.println("Waiting for messages. To exit press CTRL+C");
    } catch (Exception e) {
        System.out.println("exception");
    }

    try {
        channel.queueDeclare(COMPLETED_QUEUE, false, false, false, null);
        
    } catch (Exception e) {
        System.out.println("exception");
    }
}
}