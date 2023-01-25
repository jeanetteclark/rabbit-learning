import com.rabbitmq.client.*;
import java.io.*;

public class Worker {

  private static final String TASK_QUEUE_NAME = "task_queue";
  private static final String COMPLETED_QUEUE = "completed_queue";
  private static com.rabbitmq.client.Connection connection;
  private static com.rabbitmq.client.Channel channel;
  private static String final_message = "";

  public static void main(String[] argv) throws Exception {

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    final Connection connection = factory.newConnection();
    final Channel channel = connection.createChannel();

    channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    channel.basicQos(1);

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");

        System.out.println(" [x] Received '" + message + "'");
        try {
            doWork(message);
        } catch (Exception e){
            System.out.println("Unable to send to Completed");
        }

        try {
            String message_final = "Pass it along";
            channel.basicPublish("", COMPLETED_QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, message_final.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + message + "'");
        } catch (Exception e){
            System.out.println("Unable to send to Completed");
        } finally {
            System.out.println(" [x] Done");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
    };
    channel.basicConsume(TASK_QUEUE_NAME, false, deliverCallback, consumerTag -> { });
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

  public static void setupQueues() throws IOException {
    ConnectionFactory factory = new ConnectionFactory();
    
    try {
        factory.setHost("localhost");
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        channel.basicQos(1);
    } catch(Exception e){
        System.out.println("Unable to connect to RabbitMQ");
    }
  }
}