package com.IDP.location_service.controller;

import com.IDP.location_service.model.NearbyVehicle;
import com.IDP.location_service.model.VehicleLocation;
import com.IDP.location_service.service.LocationService;
import com.IDP.location_service.util.JwtUtil;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/v1/locations")
@CrossOrigin(origins = "*")
public class LocationController {

    private final LocationService locationService;
    private final JwtUtil jwtUtil; // Add the utility

    public LocationController(LocationService locationService, JwtUtil jwtUtil) {
        this.locationService = locationService;
        this.jwtUtil = jwtUtil;
    }

    @PostMapping("/update")
    public Mono<String> updateLocation(
            @RequestHeader("Authorization") String authHeader,
            @RequestBody VehicleLocation location) {

        // 1. Check if the header exists and starts with "Bearer "
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Missing or invalid Authorization header"));
        }

        String token = authHeader.substring(7); // Remove "Bearer "

        try {
            // 2. Validate the token. If it fails, this throws an exception.
            String secureSessionId = jwtUtil.extractAndValidateSessionId(token);

            // 3. Ensure they aren't using a valid token to update someone else's location!
            if (!secureSessionId.equals(location.sessionId())) {
                return Mono.error(new ResponseStatusException(HttpStatus.FORBIDDEN, "Token does not match session ID"));
            }

            return locationService.updateLocation(location)
                    .thenReturn("Location updated securely on the radar.");

        } catch (Exception e) {
            return Mono.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid or expired JWT Token"));
        }
    }

    @GetMapping(value = "/nearby", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<List<NearbyVehicle>> getNearbyVehicles(@RequestParam String token) {
        try {
            // Validate before opening the stream
            String sessionId = jwtUtil.extractAndValidateSessionId(token);
            return locationService.streamNearbyVehicles(sessionId);
        } catch (Exception e) {
            return Flux.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid or expired JWT Token"));
        }
    }
}