package com.IDP.location_service.config;

import com.IDP.location_service.websocket.LocationWebSocketHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class WebSocketConfig {

    @Bean
    public HandlerMapping webSocketMapping(LocationWebSocketHandler locationWebSocketHandler) {
        Map<String, WebSocketHandler> map = new HashMap<>();
        // This is the URL clients will connect to: ws://localhost:8090/ws/locations
        map.put("/ws/locations", locationWebSocketHandler);

        SimpleUrlHandlerMapping handlerMapping = new SimpleUrlHandlerMapping();
        handlerMapping.setOrder(-1); // Ensure this is checked before standard REST controllers
        handlerMapping.setUrlMap(map);

        // CORS Setup to allow Postman and Frontend
        CorsConfiguration corsConfig = new CorsConfiguration();
        corsConfig.addAllowedOriginPattern("*"); // Allow all origins
        corsConfig.addAllowedHeader("*");
        corsConfig.addAllowedMethod("*");

        // 🚨 FIX: Applied to 'handlerMapping' (Matched the variable name)
        handlerMapping.setCorsConfigurations(Map.of("/ws/locations", corsConfig));

        return handlerMapping;
    }

    // This adapter is required for WebFlux to process WebSocket requests
    @Bean
    public WebSocketHandlerAdapter handlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}