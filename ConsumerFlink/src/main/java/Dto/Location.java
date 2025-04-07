package Dto;


import lombok.Data;

@Data
public class Location{
    Double lat;
    Double lon;
//    private long timestamp; // Use long for epoch milliseconds

    public Location(Double lat, Double lon) {
        this.lat = lat;
        this.lon = lon;
//        this.timestamp = timestamp;
    }
}
