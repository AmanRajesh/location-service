package com.IDP.location_service.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

// This tells Spring not to panic if extra fields are ever added in the future!
@JsonIgnoreProperties(ignoreUnknown = true)
public record JourneyEvent(
        String eventType,
        String sessionId,
        String vehicleType,
        String timestamp
) {}