package Deserializer;

import Dto.CrimeRecord;
import Dto.Location;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.HashMap;
import java.util.Map;

public class CrimeRecordSerializer {
    private static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    public static String convertCrimeToJson(Location location) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> root = new HashMap<>();
            root.put("location", location); // location is your Location object
            return mapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON serialization error", e);
        }
    }
}
