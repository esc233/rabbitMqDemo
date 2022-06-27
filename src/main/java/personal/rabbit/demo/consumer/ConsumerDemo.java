package personal.rabbit.demo.consumer;

import com.google.common.base.Joiner;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import personal.rabbit.demo.common.MqBase;

import java.io.IOException;
import java.util.UUID;

import static personal.rabbit.demo.common.Infos.queue_direct_demo_1;

public class ConsumerDemo extends MqBase {

    class DemoConsumer extends DefaultConsumer {
        private String queueName;
        public DemoConsumer(Channel channel,String queueName) {
            super(channel);
            this.queueName = queueName;
        }
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            System.out.println(Joiner.on("|").join(queueName,
                    consumerTag,envelope.getExchange(),
                    envelope.getRoutingKey(),
                    envelope.getDeliveryTag(),
                    new String(body,"UTF-8")));
//            try {
//                Thread.sleep(5000);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
        }
    }

    public void initConsumer(String queue) throws IOException {
        Channel channel2 = getChannel();
        channel2.basicConsume(queue,new DemoConsumer(channel2,queue+ UUID.randomUUID().toString()));
    }

    public static void main(String[] args) throws IOException {
        ConsumerDemo consumerDemo1 = new ConsumerDemo();
        consumerDemo1.initConsumer(queue_direct_demo_1);
        ConsumerDemo consumerDemo2 = new ConsumerDemo();
        consumerDemo2.initConsumer(queue_direct_demo_1);
    }
}
