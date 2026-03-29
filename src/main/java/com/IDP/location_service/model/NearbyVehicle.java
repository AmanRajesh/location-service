package com.IDP.location_service.model;

public record NearbyVehicle(
        String sessionId,
        String vehicleType,
        double distanceMeters,
        double latitude,   // NEW: For exact map plotting
        double longitude   // NEW: For exact map plotting
) {}