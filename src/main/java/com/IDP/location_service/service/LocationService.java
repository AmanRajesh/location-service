package com.IDP.location_service.service;

import com.IDP.location_service.model.NearbyVehicle;
import com.IDP.location_service.model.VehicleLocation;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.IDP.location_service.model.MapVehicle;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Point;
import reactor.core.publisher.Flux;
import java.time.Duration;

@Service
public class LocationService {

    private final ReactiveRedisTemplate<String, Object> redisTemplate;
    private final AnomalyEventPublisher anomalyPublisher; // 🚨 Added Publisher
    private static final String OFFLINE_KEY = "offline_vehicles";
    private static final String TOMBSTONE_PREFIX = "journey:ended:";
    private static final String GEO_KEY = "vehicle_locations";
    private static final String PREDICTED_GEO_KEY = "predicted_locations";
    // Redis Keys
    private static final String HEARTBEAT_KEY = "active_heartbeats"; // 🚨 New ZSet Key

    // Thresholds
    private static final long STATIONARY_THRESHOLD_MS = Duration.ofSeconds(15).toMillis();
    private static final long SIGNAL_LOST_THRESHOLD_MS = Duration.ofSeconds(20).toMillis();

    public LocationService(ReactiveRedisTemplate<String, Object> redisTemplate, AnomalyEventPublisher anomalyPublisher) {
        this.redisTemplate = redisTemplate;
        this.anomalyPublisher = anomalyPublisher;
    }
    public Flux<MapVehicle> getUnifiedMapData() {
        return Flux.merge(
                fetchFromRedis(GEO_KEY, "ACTIVE"),
                fetchFromRedis(PREDICTED_GEO_KEY, "PREDICTED")
        )
                .groupBy(MapVehicle::sessionId)
                .flatMap(group -> group.reduce((vehicle1, vehicle2) -> {
                    return "ACTIVE".equals(vehicle1.status()) ? vehicle1 : vehicle2;
                }));
    }

    private Flux<MapVehicle> fetchFromRedis(String key, String status) {
        return redisTemplate.opsForZSet().range(key, Range.unbounded())
                .flatMap(memberObj -> {
                    String member = String.valueOf(memberObj);

                    // In Reactive Redis, position() for a single member returns Mono<Point>
                    return redisTemplate.opsForGeo().position(key, member)
                            .flatMap(pointObj -> {
                                // 1. If the point doesn't exist, safely skip it
                                if (pointObj == null) {
                                    return Mono.empty();
                                }

                                // 2. Cast it directly to a Point (No Lists!)
                                org.springframework.data.geo.Point p = (org.springframework.data.geo.Point) pointObj;

                                // 3. Map it to our DTO
                                return Mono.just(parseToMapVehicle(member, p, status));
                            });
                });
    }

    private MapVehicle parseToMapVehicle(String member, org.springframework.data.geo.Point point, String status) {
        // 1. Strip out any accidental JSON quotes
        String cleanMember = member.replace("\"", "").trim();
        String[] parts = cleanMember.split(":");

        // 2. Safely extract base strings
        String vehicleType = parts.length > 0 ? parts[0] : "unknown";
        String sessionId = parts.length > 1 ? parts[1] : "unknown_session";

        double speed = 0.0;
        double bearing = 0.0;

        // 3. Safely parse numbers and log any weird data
        try {
            speed = parts.length > 2 ? Double.parseDouble(parts[2]) : 0.0;
            bearing = parts.length > 3 ? Double.parseDouble(parts[3]) : 0.0;
        } catch (Exception e) {
            System.err.println("⚠️ Failed to parse speed/bearing from member: " + cleanMember);
        }

        return new MapVehicle(
                sessionId,
                vehicleType,
                point.getY(), // Latitude
                point.getX(), // Longitude
                bearing,
                speed,
                status
        );
    }


    // ==========================================
    // 1. UPDATE LOCATION & ANOMALY CHECKS
    // ==========================================
    public Mono<Void> updateLocation(VehicleLocation location) {
        String sessionId = location.sessionId();
        String tombstoneKey = TOMBSTONE_PREFIX + sessionId;

        return redisTemplate.hasKey(tombstoneKey)
                .flatMap(isDead -> {
                    if (Boolean.TRUE.equals(isDead)) {
                        // This journey was ended. Ignore this malicious/zombie ping entirely!
                        System.out.println("🛡️ [SECURITY] Blocked ghost ping for ended session: " + sessionId);
                        return Mono.empty();
                    }
                    long now = System.currentTimeMillis();
                    String trailKey = "trail:" + sessionId;
                    String memberValue = location.vehicleType() + ":" + sessionId;
                    Point point = new Point(location.longitude(), location.latitude());

                    // 🚨 A. Perform stationary check FIRST (before we push the new location to the trail)
                    Mono<Void> stationaryCheck = redisTemplate.opsForList().index(trailKey, 0)
                            .cast(VehicleLocation.class)
                            .flatMap(lastLoc -> processStationaryLogic(location, lastLoc, now))
                            .switchIfEmpty(Mono.empty()); // If trail is empty (first update), do nothing

                    // B. Update the Live Geo Map (Overwrites old location)
                    Mono<Long> updateGeoMap = redisTemplate.opsForGeo()
                            .add(GEO_KEY, point, memberValue);
                    Mono<Void> checkRecovery = redisTemplate.opsForSet().isMember(OFFLINE_KEY, sessionId)
                            .flatMap(isOffline -> {
                                if (Boolean.TRUE.equals(isOffline)) {
                                    anomalyPublisher.publishSignalRestored(sessionId);
                                    return redisTemplate.opsForSet().remove(OFFLINE_KEY, sessionId).then();
                                }
                                return Mono.empty();
                            });
                    // C. Save a "Breadcrumb Trail"
                    Mono<Long> saveHistory = redisTemplate.opsForList()
                            .leftPush(trailKey, location)
                            .flatMap(size -> redisTemplate.opsForList().trim(trailKey, 0, 9))
                            .then(redisTemplate.expire(trailKey, Duration.ofHours(24)))
                            .thenReturn(1L);

                    // 🚨 D. Update Heartbeat for Signal Lost detection
                    Mono<Boolean> updateHeartbeat = redisTemplate.opsForZSet()
                            .add(HEARTBEAT_KEY, sessionId, now);

                    // Execute the stationary check first, THEN update the map, trail, and heartbeat concurrently
                    return stationaryCheck.then(Mono.when(updateGeoMap, saveHistory, updateHeartbeat, checkRecovery));
                });
    }

    private Mono<Void> processStationaryLogic(VehicleLocation newLoc, VehicleLocation lastLoc, long now) {
        String stationaryKey = "stationary_since:" + newLoc.sessionId();

        if (lastLoc.latitude() == newLoc.latitude() && lastLoc.longitude() == newLoc.longitude()) {
            // Vehicle hasn't moved. Check how long it's been here.
            return redisTemplate.opsForValue().get(stationaryKey)
                    .cast(Long.class)
                    .switchIfEmpty(
                            // First time noticing they are stopped: save the timestamp with a 1-hour TTL
                            redisTemplate.opsForValue().set(stationaryKey, now, Duration.ofHours(1)).then(Mono.empty())
                    )
                    .flatMap(startTime -> {
                        if ((now - startTime) > STATIONARY_THRESHOLD_MS) {
                            // Stationary too long! Publish event.
                            anomalyPublisher.publishPossibleAccident(newLoc.sessionId(), newLoc.latitude(), newLoc.longitude());
                            // Reset the timer to avoid spamming the Kafka topic every second
                            return redisTemplate.delete(stationaryKey).then();
                        }
                        return Mono.empty();
                    });
        } else {
            // They moved! Delete the stationary timer if it exists.
            return redisTemplate.delete(stationaryKey).then();
        }
    }

    // ==========================================
    // 2. REMOVE VEHICLE (CLEANUP)
    // ==========================================
    public Mono<Void> removeVehicle(String vehicleType, String sessionId) {
        String memberValue = vehicleType + ":" + sessionId;
        String trailKey = "trail:" + sessionId;
        String stationaryKey = "stationary_since:" + sessionId;
        String tombstoneKey = TOMBSTONE_PREFIX + sessionId;

        Mono<Long> removeGeo = redisTemplate.opsForGeo().remove(GEO_KEY, memberValue);
        Mono<Long> removePredicted = redisTemplate.opsForGeo().remove(PREDICTED_GEO_KEY, memberValue); // 🚨 2. Kill the Ghost Truck!
        Mono<Long> removeTrail = redisTemplate.delete(trailKey);
        Mono<Long> removeOffline = redisTemplate.opsForSet().remove(OFFLINE_KEY, sessionId);
        Mono<Boolean> plantTombstone = redisTemplate.opsForValue()
                .set(tombstoneKey, "DEAD", Duration.ofHours(24));
        // 🚨 Also clean up heartbeat and stationary keys so we don't get false positives
        Mono<Long> removeHeartbeat = redisTemplate.opsForZSet().remove(HEARTBEAT_KEY, sessionId);
        Mono<Long> removeStationary = redisTemplate.delete(stationaryKey);

        return Mono.when(
                removeGeo,
                removePredicted,
                removeTrail,
                removeHeartbeat,
                removeStationary,
                removeOffline,
                plantTombstone
        );
    }

    // ==========================================
    // 3. STREAM NEARBY VEHICLES
    // ==========================================
    public Flux<java.util.List<NearbyVehicle>> streamNearbyVehicles(String sessionId) {
        return Flux.interval(Duration.ofSeconds(2))
                .flatMap(tick -> {
                    return redisTemplate.opsForZSet().range(GEO_KEY, Range.closed(0L, -1L))
                            .filter(member -> String.valueOf(member).endsWith(":" + sessionId))
                            .next()
                            .flatMap(this::findVehiclesNearby)
                            .defaultIfEmpty(java.util.List.of());
                });
    }

    private Mono<java.util.List<NearbyVehicle>> findVehiclesNearby(Object memberObj) {
        String memberString = String.valueOf(memberObj);

        RedisGeoCommands.GeoRadiusCommandArgs args = RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                .includeDistance()
                .includeCoordinates()
                .sortAscending();

        return redisTemplate.opsForGeo()
                .radius(GEO_KEY, memberString, new Distance(1.0, Metrics.KILOMETERS), args)
                .filter(result -> !String.valueOf(result.getContent().getName()).equals(memberString))
                .map(result -> {
                    String[] parts = String.valueOf(result.getContent().getName()).split(":");
                    String vehicleType = parts[0];
                    String foundSessionId = parts[1];
                    Point pt = result.getContent().getPoint();

                    return new NearbyVehicle(
                            foundSessionId,
                            vehicleType,
                            result.getDistance().getValue(),
                            pt.getY(),
                            pt.getX()
                    );
                })
                .collectList();
    }

    // ==========================================
    // 4. BACKGROUND SCANNER FOR SIGNAL LOST
    // ==========================================
    // 🚨 Runs every 30 seconds to check for dead heartbeats
    @Scheduled(fixedRate = 5000)
    public void scanForLostSignals() {
        long cutoffTime = System.currentTimeMillis() - SIGNAL_LOST_THRESHOLD_MS;

        redisTemplate.opsForZSet()
                // 1. Get the list of all expired session IDs first
                .rangeByScore(HEARTBEAT_KEY, org.springframework.data.domain.Range.closed(0.0, (double) cutoffTime))
                .collectList()
                .flatMapMany(Flux::fromIterable) // 2. Now process them one by one
                .flatMap(sessionIdObj -> {
                    String sessionId = String.valueOf(sessionIdObj);
                    String trailKey = "trail:" + sessionId;

                    return redisTemplate.opsForList().range(trailKey, 0, -1)
                            .cast(VehicleLocation.class)
                            .collectList()
                            .flatMap(trail -> {
                                // 3. Publish the Snapshot
                                anomalyPublisher.publishSignalLostWithHistory(sessionId, trail);
                                String vehicleType = trail.isEmpty() ? "unknown" : trail.get(0).vehicleType();
                                String memberValue = vehicleType + ":" + sessionId;
                                // 4. Cleanup
                                return Mono.when(
                                        redisTemplate.opsForSet().add(OFFLINE_KEY, sessionId),
                                        redisTemplate.opsForZSet().remove(HEARTBEAT_KEY, sessionId),
                                        redisTemplate.opsForGeo().remove(GEO_KEY, memberValue)
                                );
                            });
                })
                .subscribe(
                        null,
                        err -> System.err.println("❌ Error in Signal Scan: " + err.getMessage()),
                        () -> System.out.println("🔭 Signal scan complete.")
                );
    }
}