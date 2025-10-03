package br.com.fiap.kafkademo.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaMessageProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topicName;

    public KafkaMessageProducer(@Value("${app.kafka.topic.meu-topico}") String topicName, KafkaTemplate<String, String> kafkaTemplate){
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }


    public void sendMessage(String message){
        log.info("Enviando a mensagem '{}' para o tópico '{}'", message, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.whenComplete((result, ex) -> {
            if(ex == null){
                log.info("Mensagem enviada com sucesso para o tópico '{}' partição '{}' com offset '{}'", result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }else{
                log.info("Mensagem não enviada");
            }
        });
    }


    public void sendMessageWithKey(String key, String message){
        log.info("Enviando a mensagem '{}', com chave '{}' para o tópico '{}'", message, key, topicName);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, key, message);
        future.whenComplete((result, ex) -> {
            if(ex == null){
                log.info("Mensagem com chave '{}' enviada com sucesso para o tópico '{}' partição '{}' com offset '{}'", key, result.getRecordMetadata().topic(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }else{
                log.info("Mensagem não enviada");
            }
        });
    }
}
