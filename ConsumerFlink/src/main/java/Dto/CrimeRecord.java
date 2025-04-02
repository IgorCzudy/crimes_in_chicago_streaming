package Dto;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;


//import java.time.LocalDateTime;
//import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule.LocalDateTime


@Data
public class CrimeRecord {

    @JsonProperty("ID")
    private int id;

    @JsonProperty("Case_Number")
    private String caseNumber;

    @JsonProperty("Date")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private String date;

    @JsonProperty("Block")
    private String block;

    @JsonProperty("IUCR")  // Add @JsonProperty to map the field properly
    private String iucr;

    @JsonProperty("Primary_Type")
    private String primaryType;

    @JsonProperty("Description")
    private String description;

    @JsonProperty("Location_Description")
    private String locationDescription;

    @JsonProperty("Arrest")
    private boolean arrest;

    @JsonProperty("Domestic")
    private boolean domestic;

    @JsonProperty("Beat")
    private int beat;

    @JsonProperty("District")
    private Integer district;

    @JsonProperty("Ward")
    private Integer ward;

    @JsonProperty("Community_Area")
    private Integer communityArea;

    @JsonProperty("FBI_Code")
    private String fbicode;

    @JsonProperty("X_Coordinate")
    private Integer xCoordinate;

    @JsonProperty("Y_Coordinate")
    private Integer yCoordinate;

    @JsonProperty("Year")
    private int year;

    @JsonProperty("Updated_On")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private String updatedOn;

    @JsonProperty("Latitude")
    private Double latitude;

    @JsonProperty("Longitude")
    private Double longitude;

    @JsonProperty("Location")
    private String location;
}

