package com.IDP.location_service.model;

public record MapVehicle(
        String sessionId,
        String vehicleType,
        double latitude,
        double longitude,
        double bearing,
        double speed,
        String status // "ACTIVE" or "PREDICTED"
) {}