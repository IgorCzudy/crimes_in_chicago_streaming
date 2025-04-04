package Dto;

import lombok.AllArgsConstructor;
import lombok.Data;


@AllArgsConstructor
@Data
public class AgregatedCrimeRecord {
    private int crimeSum;
    private int domesticCrimeSum;
    private Integer district;
    private String windowStartDate;
    private String windowEndDate;


    @Override
    public String toString() {
        return "AgregatedCrimeRecord{" +
                "crimeSum=" + crimeSum +
                ", domesticCrimeSum=" + domesticCrimeSum +
                ", district=" + district +
                ", windowStartDate=" + windowStartDate +
                ", windowEndDate='" + windowEndDate + '\'' +
                '}';
    }

}
