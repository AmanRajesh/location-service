package com.IDP.location_service.controller;

import com.IDP.location_service.model.NearbyVehicle;
import com.IDP.location_service.model.VehicleLocation;
import com.IDP.location_service.service.LocationService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/v1/locations")
@CrossOrigin(origins = "*")
public class LocationController {

    private final LocationService locationService;

    public LocationController(LocationService locationService) {
        this.locationService = locationService;
    }

    // App POSTs its coordinates here
    @PostMapping("/update")
    public Mono<String> updateLocation(@RequestBody VehicleLocation location) {
        return locationService.updateLocation(location)
                .thenReturn("Location updated on the radar.");
    }

    // App opens a persistent connection here (Server-Sent Events)
    @GetMapping(value = "/nearby", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<List<NearbyVehicle>> getNearbyVehicles(@RequestParam String sessionId) {
        return locationService.streamNearbyVehicles(sessionId);
    }
}