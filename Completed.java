import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.nio.charset.StandardCharsets;

public class Completed {

    private final static String QUEUE_COMPLETED = "completed_queue";
    private final static String QUEUE_WORKER = "quality_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_COMPLETED, false, false, false, null);
        channel.queueDeclare(QUEUE_WORKER, true, false, false, null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            try{
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println(" [x] Received '" + message + "'");
            } finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }

        };
        channel.basicConsume(QUEUE_COMPLETED, false, deliverCallback, consumerTag -> { });
    }
}