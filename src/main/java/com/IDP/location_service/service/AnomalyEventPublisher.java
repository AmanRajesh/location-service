package com.IDP.location_service.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class AnomalyEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public AnomalyEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishSignalLost(String sessionId) {
        System.out.println("⚠️ [ALERT] Signal lost for session: " + sessionId);
        // You would likely pass a JSON string or a specific Java Record here
        kafkaTemplate.send("prediction-events", sessionId, "{\"eventType\":\"SIGNAL_LOST\", \"sessionId\":\"" + sessionId + "\"}");
    }

    public void publishPossibleAccident(String sessionId, double lat, double lon) {
        System.out.println("🚨 [ALERT] Possible accident/stationary vehicle for session: " + sessionId);
        kafkaTemplate.send("prediction-events", sessionId, "{\"eventType\":\"POSSIBLE_ACCIDENT\", \"sessionId\":\"" + sessionId + "\", \"lat\":" + lat + ", \"lon\":" + lon + "}");
    }

    public void publishSignalRestored(String sessionId) {
        System.out.println("📶 [RESTORED] Signal back online for session: " + sessionId);
        kafkaTemplate.send("prediction-events", sessionId, "{\"eventType\":\"SIGNAL_RESTORED\", \"sessionId\":\"" + sessionId + "\"}");
    }
}