package Dto;


import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class AnomaliaRecord {

    private int countOfCrimes;
    private String primaryType;
    private int district;
    private String windowStartData;
    private String windowEndData;
}
