package com.IDP.location_service.model;

public record NearbyVehicle(
        String sessionId,
        double distanceKm
) {}