package com.IDP.location_service.service;

import com.IDP.location_service.model.NearbyVehicle;
import com.IDP.location_service.model.VehicleLocation;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.ReactiveGeoOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Service
public class LocationService {

    private final ReactiveGeoOperations<String, String> geoOps;
    private static final String GEO_KEY = "active_vehicles";

    public LocationService(ReactiveRedisTemplate<String, String> redisTemplate) {
        this.geoOps = redisTemplate.opsForGeo();
    }

    // 1. Plot the vehicle on the Redis spatial map
    public Mono<Void> updateLocation(VehicleLocation location) {
        // Redis requires Longitude first, then Latitude
        Point point = new Point(location.longitude(), location.latitude());

        return geoOps.add(GEO_KEY, point, location.sessionId()).then();
    }
    public Mono<Void> removeVehicle(String sessionId) {
        return geoOps.remove(GEO_KEY, sessionId).then();
    }

    // 2. Find all vehicles within 1km of a specific point
    // 2. STREAM: Find all vehicles within 1km of a SPECIFIC VEHICLE
    // Change the return type to Flux<List<NearbyVehicle>>
    public Flux<List<NearbyVehicle>> streamNearbyVehicles(String sessionId) {
        Distance radius = new Distance(1.0, Metrics.KILOMETERS);

        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs
                .newGeoRadiusArgs()
                .includeDistance()
                .sortAscending();

        return Flux.interval(Duration.ofSeconds(2))
                .flatMap(tick -> geoOps.radius(GEO_KEY, sessionId, radius, args)
                        // Map the Redis result into our new JSON Record
                        .map(geoResult -> new NearbyVehicle(
                                geoResult.getContent().getName(),
                                geoResult.getDistance().getValue()
                        ))
                        // Filter out our own vehicle
                        .filter(vehicle -> !vehicle.sessionId().equals(sessionId))
                        // THE UPGRADE: Group them into a single array before sending!
                        .collectList()
                );
    }
}