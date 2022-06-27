package personal.rabbit.demo.common;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MqBase {
    private Connection connection;

    public MqBase() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(LocalProperties.user_name);
        connectionFactory.setPassword(LocalProperties.password);
        connectionFactory.setVirtualHost("demo");
        connectionFactory.setHost(LocalProperties.rabbit_address);
        connectionFactory.setPort(LocalProperties.rabbit_port);
        try {
            connection=connectionFactory.newConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private Connection getConn(){
        return connection;
    }

    protected Channel getChannel() throws IOException {
        return connection.createChannel();
    }

}
