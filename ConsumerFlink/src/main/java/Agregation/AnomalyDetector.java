package Agregation;

import Dto.AnomaliaRecord;
import Dto.CrimeKey;
import Dto.CrimeRecord;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class AnomalyDetector extends ProcessWindowFunction<CrimeRecord, AnomaliaRecord, CrimeKey, TimeWindow> {

    private final int treshold;

    public AnomalyDetector(int treshold) {
        this.treshold = treshold;
    }

    @Override
    public void process(CrimeKey crimeKey, ProcessWindowFunction<CrimeRecord, AnomaliaRecord, CrimeKey, TimeWindow>.Context context, Iterable<CrimeRecord> iterable, Collector<AnomaliaRecord> collector) throws Exception {

        int count = 0;

        for (CrimeRecord event: iterable){
            count++;
        }
        long windowEnd = context.window().getEnd();
        Instant instantEnd = Instant.ofEpochMilli(windowEnd);
        ZonedDateTime zdtEnd = instantEnd.atZone(ZoneId.systemDefault());
        String windowEndDate = zdtEnd.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

        long windowStart = context.window().getStart();
        Instant instantStart = Instant.ofEpochMilli(windowStart);
        ZonedDateTime zdtStart = instantStart.atZone(ZoneId.systemDefault());
        String windowEndStart = zdtStart.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

        if (count > treshold){
            collector.collect(
                    new AnomaliaRecord(
                            count,
                            crimeKey.getPrimaryType(),
                            crimeKey.getDistrict(),
                            windowEndStart,
                            windowEndDate
                    )
            );
        }



    }
}
