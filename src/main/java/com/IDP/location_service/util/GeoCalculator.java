package com.IDP.location_service.util;

import org.springframework.data.geo.Point;

public class GeoCalculator {
    private static final double EARTH_RADIUS_METERS = 6371000.0;

    public static double calculateBearing(double lat1, double lon1, double lat2, double lon2) {
        double lat1Rad = Math.toRadians(lat1);
        double lat2Rad = Math.toRadians(lat2);
        double deltaLon = Math.toRadians(lon2 - lon1);

        double y = Math.sin(deltaLon) * Math.cos(lat2Rad);
        double x = Math.cos(lat1Rad) * Math.sin(lat2Rad) -
                Math.sin(lat1Rad) * Math.cos(lat2Rad) * Math.cos(deltaLon);

        double bearing = Math.atan2(y, x);
        return (Math.toDegrees(bearing) + 360) % 360;
    }

    public static double calculateDistance(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METERS * c;
    }

    public static Point calculateDestination(double lat, double lon, double bearingDegrees, double distanceMeters) {
        double radiusRatio = distanceMeters / EARTH_RADIUS_METERS;
        double bearingRad = Math.toRadians(bearingDegrees);
        double lat1Rad = Math.toRadians(lat);
        double lon1Rad = Math.toRadians(lon);

        double lat2Rad = Math.asin(Math.sin(lat1Rad) * Math.cos(radiusRatio) +
                Math.cos(lat1Rad) * Math.sin(radiusRatio) * Math.cos(bearingRad));

        double lon2Rad = lon1Rad + Math.atan2(Math.sin(bearingRad) * Math.sin(radiusRatio) * Math.cos(lat1Rad),
                Math.cos(radiusRatio) - Math.sin(lat1Rad) * Math.sin(lat2Rad));

        return new Point(Math.toDegrees(lon2Rad), Math.toDegrees(lat2Rad));
    }
}