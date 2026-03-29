package com.IDP.location_service.websocket;

import com.IDP.location_service.model.VehicleLocation;
import com.IDP.location_service.service.LocationService;
import com.IDP.location_service.util.JwtUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class LocationWebSocketHandler implements WebSocketHandler {

    private final LocationService locationService;
    private final ObjectMapper objectMapper;
    private final JwtUtil jwtUtil;

    public LocationWebSocketHandler(LocationService locationService, ObjectMapper objectMapper,JwtUtil jwtUtil) {
        this.locationService = locationService;
        this.objectMapper = objectMapper;
        this.jwtUtil = jwtUtil;

    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {

        // 1. Extract the sessionId from the connection URL
        // Example: ws://localhost:8090/ws/locations?sessionId=driver-123
        // 1. Extract the token instead of sessionId
        String token = UriComponentsBuilder.fromUri(session.getHandshakeInfo().getUri())
                .build()
                .getQueryParams()
                .getFirst("token");

        if (token == null) {
            System.err.println("❌ WS Rejected: No token provided");
            return session.close();
        }

        String sessionId;
        try {
            // 2. Validate it using your existing JwtUtil (you will need to inject JwtUtil into this class constructor)

            sessionId = jwtUtil.extractAndValidateSessionId(token);
        } catch (Exception e) {
            System.err.println("❌ WS Rejected: Invalid Token");
            return session.close();
        }

        System.out.println("🟢 WS Connected for Session: " + sessionId);

        if (sessionId == null) {
            return session.close(); // Reject connection if no session ID is provided
        }

        // ==========================================
        // PIPE 1: INCOMING (Client -> Server)
        // ==========================================
        Mono<Void> receiveStream = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(payload -> {
                    try {
                        // Parse the raw JSON directly into your VehicleLocation object
                        VehicleLocation location = objectMapper.readValue(payload, VehicleLocation.class);

                        return locationService.updateLocation(location)
                                .onErrorResume(e -> Mono.empty()); // Ignore Redis save errors to keep socket alive
                    } catch (Exception e) {
                        System.err.println("❌ WS Parse Error: " + e.getMessage());
                        return Mono.empty();
                    }
                })
                .then(); // Convert Flux to Mono<Void> indicating when the stream is done

        // ==========================================
        // PIPE 2: OUTGOING (Server -> Client)
        // ==========================================
        // Call your exact method which returns Flux<List<NearbyVehicle>> ticking every 2 seconds
        Flux<WebSocketMessage> sendStream = locationService.streamNearbyVehicles(sessionId)
                .map(vehicleList -> {
                    try {
                        // Convert the List<NearbyVehicle> into a JSON string
                        String jsonResponse = objectMapper.writeValueAsString(vehicleList);
                        return session.textMessage(jsonResponse);
                    } catch (Exception e) {
                        return session.textMessage("[]"); // Send empty array if serialization fails
                    }
                });

        // Bind the outgoing stream to the session's sender
        Mono<Void> sendMono = session.send(sendStream);

        // ==========================================
        // MERGE: Run both pipes simultaneously
        // ==========================================
        return Mono.zip(receiveStream, sendMono)
                .doFinally(sig -> System.out.println("🔴 WS Disconnected: " + sessionId))
                .then();
    }
}