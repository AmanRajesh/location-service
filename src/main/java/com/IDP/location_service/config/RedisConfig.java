package com.IDP.location_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer; // <-- Notice the '2' is gone!
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(ReactiveRedisConnectionFactory factory) {
        // 1. Keys are plain strings
        StringRedisSerializer keySerializer = new StringRedisSerializer();

        // 2. Use the new Jackson 3 builder to create the Object serializer
        GenericJacksonJsonRedisSerializer valueSerializer = GenericJacksonJsonRedisSerializer.builder()
                .enableUnsafeDefaultTyping() // Reproduces the old default behavior where it saves the @class type into the JSON
                .build();

        // 3. Build the serialization context
        RedisSerializationContext.RedisSerializationContextBuilder<String, Object> builder =
                RedisSerializationContext.newSerializationContext(keySerializer);

        RedisSerializationContext<String, Object> context = builder.value(valueSerializer).build();

        // 4. Return the fully configured bean
        return new ReactiveRedisTemplate<>(factory, context);
    }
}