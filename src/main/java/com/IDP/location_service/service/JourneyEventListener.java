package com.IDP.location_service.service;

import com.IDP.location_service.model.JourneyEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class JourneyEventListener {

    private final LocationService locationService;

    public JourneyEventListener(LocationService locationService) {
        this.locationService = locationService;
    }

    @KafkaListener(topics = "journey-events", groupId = "location-service-group")
    public void consumeJourneyEvent(JourneyEvent event) {

        System.out.println("🎧 [LOCATION SERVICE] Received: " + event.eventType() + " for Session: " + event.sessionId());

        if ("JOURNEY_ENDED".equals(event.eventType())) {
            locationService.removeVehicle(event.vehicleType(),event.sessionId())
                    .doOnSuccess(v -> System.out.println("✅ Ghost Vehicle eliminated from radar."))
                    .doOnError(e -> System.err.println("❌ Failed to remove vehicle: " + e.getMessage()))
                    .block();
        }
    }
}