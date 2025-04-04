package Dto;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class CrimeKey {
    private String primaryType;
    private int district;
}
