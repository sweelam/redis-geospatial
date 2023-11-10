package com.example.newFeatures.rest;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.core.GeoOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.domain.geo.Metrics;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/location")
public class LocationController {
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisTemplate<String, String> redisTemplateText;

    public LocationController(RedisTemplate<String, Object> redisTemplate,
            RedisTemplate<String, String> redisTemplateText) {
        this.redisTemplate = redisTemplate;
        this.redisTemplateText = redisTemplateText;
    }

    @PostMapping("/")
    public ResponseEntity<String> addNewLocation(@RequestBody List<Location> location) {

        location.parallelStream()
                .forEach(this::addNewLocation);

        return ResponseEntity.ok("Geo Locations Added Successfully");
    }

    @PostMapping("/{stateName}")
    public ResponseEntity<List<Location>> getLocation(@PathVariable String stateName,
            @RequestParam(name = "km", defaultValue = "100.00") int km, @RequestBody Location location) {
        GeoOperations<String, Object> geoRedis = redisTemplate.opsForGeo();

        var point = new Point(Double.valueOf(location.lng()), Double.valueOf(location.lat()));
        var circle = new Circle(point, new Distance(Double.valueOf(km), Metrics.KILOMETERS));

        var geoRadiusCommandArgs = GeoRadiusCommandArgs.newGeoRadiusArgs().sortDescending();

        var startTime = System.nanoTime();
        var result = geoRedis.radius(location.countryName, circle, geoRadiusCommandArgs);
        System.out.println("Time taken by cache " + Duration.ofNanos(System.nanoTime() - startTime).toMillis() + " ms");


        var response = Optional.ofNullable(result)
                .map(r -> r.getContent()
                        .stream()
                        .map(GeoResult::getContent)
                        .map(t -> (Location) t.getName())
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());

        return ResponseEntity.ok(response);
    }

    @PostMapping("/v2/{stateName}")
    public ResponseEntity<List<Location>> getLocationBoxed(@PathVariable String stateName,
            @RequestParam(name = "km", defaultValue = "100.00") int km, @RequestBody Location location) {
        GeoOperations<String, Object> geoRedis = redisTemplate.opsForGeo();

        var point = new Point(Double.valueOf(location.lng()), Double.valueOf(location.lat()));
        var circle = new Circle(point, new Distance(Double.valueOf(km), Metrics.KILOMETERS));

        var startTime = System.nanoTime();
        var result = geoRedis.search(location.countryName, circle);
        System.out.println("Time taken by cache " + Duration.ofNanos(System.nanoTime() - startTime).toMillis() + " ms");

        var response = Optional.ofNullable(result)
                .map(r -> r.getContent()
                        .stream()
                        .map(GeoResult::getContent)
                        .map(t -> (Location) t.getName())
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());

        return ResponseEntity.ok(response);
    }

    private void addNewLocation(Location l) {
        GeoOperations<String, Object> geoOps = redisTemplate.opsForGeo();

        var point = new Point(Double.valueOf(l.lat()), Double.valueOf(l.lng()));

        geoOps.add(l.countryName(), point, l);

        return;
    }

    public record Location(Integer id, String stateName, String countryCode,
            String countryName, String lat, String lng) {
    }
}
