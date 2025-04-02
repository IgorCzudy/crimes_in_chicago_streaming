package Deserializer;

import Dto.CrimeRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<CrimeRecord> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public CrimeRecord deserialize(byte[] bytes) throws IOException {
        try {
            return objectMapper.readValue(bytes, CrimeRecord.class);
        } catch (IOException e) {
            throw e; // Re-throw to let Flink handle the error
        }

    }


    @Override
    public boolean isEndOfStream(CrimeRecord crimeRecord) {
        return false;
    }

    @Override
    public TypeInformation<CrimeRecord> getProducedType() {
        return TypeInformation.of(CrimeRecord.class);
    }
}
