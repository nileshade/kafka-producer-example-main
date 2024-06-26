package com.fashionsincity.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.fashionsincity.dto.Customer;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String,Object> template;

//    public void sendMessageToTopic(String message){
//        
//    	 //CompletableFuture<SendResult<String, Object>> future = template.send("topic1", 2 ,null ,message); //to send msg to specific topic
//    	CompletableFuture<SendResult<String, Object>> future = template.send("topic1", message);
//          future.whenComplete((result,ex)->{
//            if (ex == null) {
//            	System.out.println("Sent message=[" + message +
//                        "] to topic=[" + result.getRecordMetadata().topic() +
//                        "], partition=[" + result.getRecordMetadata().partition() +
//                        "], offset=[" + result.getRecordMetadata().offset() +
//                        "], timestamp=[" + result.getRecordMetadata().timestamp() + "]");
//            } else {
//                System.out.println("Unable to send message=[" +
//                        message + "] due to : " + ex.getMessage());
//            }
//        });
//
//    }

    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("topic2", customer);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + customer.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            customer.toString() + "] due to : " + ex.getMessage());
                }
            });

        } catch (Exception ex) {
            System.out.println("ERROR : "+ ex.getMessage());
        }
    }
}
