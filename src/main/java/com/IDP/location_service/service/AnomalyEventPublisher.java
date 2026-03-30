package com.IDP.location_service.service;

import com.IDP.location_service.model.VehicleLocation;
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
    public void publishSignalLostWithHistory(String sessionId, java.util.List<VehicleLocation> trail) {
        // We grab the most recent point from the trail for convenience
        Object lastSeen = trail.isEmpty() ? "unknown" : trail.get(0);

        // Construct a JSON payload that includes the last known state
        String payload = String.format(
                "{\"eventType\":\"SIGNAL_LOST\", \"sessionId\":\"%s\", \"lastSeen\":%s, \"trail\":%s}",
                sessionId,
                serializeToJson(lastSeen),
                serializeToJson(trail)
        );

        kafkaTemplate.send("prediction-events", sessionId, payload);
        System.out.println("📤 [KAFKA] Published SIGNAL_LOST with history for: " + sessionId);
    }

    // Helper to ensure objects are stringified properly if not using a dedicated JSON library here
    private String serializeToJson(Object obj) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }
}