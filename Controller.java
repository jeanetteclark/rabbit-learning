import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class Controller {

  private static final String QUEUE_WORKER = "quality_queue";
  private static final String QUEUE_COMPLETED = "completed_queue";
  private static com.rabbitmq.client.Connection connection;
  private static com.rabbitmq.client.Channel channel;

  public static void main(String[] argv) throws Exception {

    Controller ctl = new Controller();
    ctl.setupQueues();

    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
      try {
        String message2 = new String(delivery.getBody(), StandardCharsets.UTF_8);
        System.out.println(" [x] Received '" + message2 + "'");
      } finally {
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }

    };
    channel.basicConsume(QUEUE_COMPLETED, false, deliverCallback, consumerTag -> {
    });

    Boolean stop = true;
    // Continue to listen on specified port for client messages
    while (stop) {
      int portNumber = Integer.parseInt(argv[0]);

      ServerSocket serverSocket = new ServerSocket(portNumber);
      Socket clientSocket = serverSocket.accept();

      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

      String info = in.readLine();

      String message = String.join(" ", info);

      channel.basicPublish("", QUEUE_WORKER,
          MessageProperties.PERSISTENT_TEXT_PLAIN,
          message.getBytes("UTF-8"));
      System.out.println(" [x] Sent '" + message + "'");

      // Client has indicated a stop for all tests, as the first line sent to a new
      // connection.
      clientSocket.close();
      serverSocket.close();
    }

  }

  public void setupQueues() throws IOException {

    /*
     * Connect to the RabbitMQ queue containing entries for which quality reports
     * need to be created.
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