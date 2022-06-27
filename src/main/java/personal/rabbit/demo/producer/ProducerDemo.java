package personal.rabbit.demo.producer;

import com.google.common.base.Joiner;
import com.rabbitmq.client.*;
import personal.rabbit.demo.common.MqBase;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static personal.rabbit.demo.common.Infos.*;

public class ProducerDemo extends MqBase {

    public void declare() throws IOException {
        Channel channel = getChannel();
        channel.exchangeDeclare(exchange_direct_demo,"direct");
        channel.exchangeDeclare(exchange_topic_demo,"topic");
        channel.queueDeclare(queue_direct_demo_1,false,false,true,null);
        channel.queueDeclare(queue_direct_demo_2,false,false,true,null);
        channel.queueDeclare(queue_topic_demo_1,false,false,true,null);
        channel.queueDeclare(queue_topic_demo_2,false,false,true,null);
        channel.queueBind(queue_direct_demo_1,exchange_direct_demo,routing_direct_key_1);
        channel.queueBind(queue_direct_demo_2,exchange_direct_demo,routing_direct_key_2);
        channel.queueBind(queue_topic_demo_1,exchange_topic_demo,routing_topic_key_1);
        channel.queueBind(queue_topic_demo_2,exchange_topic_demo,routing_topic_key_2);
    }

    public void publish(String exchange,String routeKey,String word) throws IOException {
        Channel channel = getChannel();
        channel.basicPublish(exchange,routeKey,null,word.getBytes());
    }

    public void publishTx(String exchange,String routeKey,String word,boolean commit) throws IOException {
        Channel channel = getChannel();
        channel.txSelect();
        System.out.println("事务开始");
        channel.basicPublish(exchange,routeKey,null,word.getBytes());
        if(commit){
            channel.txCommit();
            System.out.println("事务提交");
        }else {
            channel.txRollback();
            System.out.println("事务回滚");
        }
        channel = getChannel();
        channel.basicPublish(exchange,routeKey,null,"事务结束".getBytes());
    }


    public static void main(String[] args) throws IOException, TimeoutException {
        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.declare();
        for (int i=0;i<50;i++) {
            producerDemo.publish(exchange_direct_demo, routing_direct_key_1, "test1"+i);
//            producerDemo.publish(exchange_direct_demo, routing_direct_key_2, "test2"+i);
        }
        System.out.println("发送结束");
    }
}
