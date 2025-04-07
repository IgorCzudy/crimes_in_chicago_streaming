/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package FlinkConsumer;

import Agregation.AnomalyDetector;
import Deserializer.JSONValueDeserializationSchema;
import Deserializer.CrimeRecordSerializer;
import Dto.*;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilderBase;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;

import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.geo.GeoPoint;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.jdbc.JdbcSink;


import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class StreamingJob {

	public static void main(String[] args) throws Exception {
		System.setProperty("log4j.configurationFile", "log4j.properties");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		String topic = "crime";
		String bootstrapServers = "localhost:9092";  // Kafka server address

		KafkaSource<CrimeRecord> source = KafkaSource.<CrimeRecord>builder()
					.setBootstrapServers(bootstrapServers)
					.setTopics(topic)
					.setGroupId("test-group")
					.setStartingOffsets(OffsetsInitializer.earliest())
					.setValueOnlyDeserializer(new JSONValueDeserializationSchema()) // Custom deserialization for Order
					.build();

				WatermarkStrategy<CrimeRecord> watermarkStrategy =
				WatermarkStrategy.<CrimeRecord>forMonotonousTimestamps()
						.withTimestampAssigner((record, ts) -> {
							String dateString = record.getDate();
							DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
							LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);
							return dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
						});

		DataStream<CrimeRecord> crimeRecordStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

		DataStream<AgregatedCrimeRecord> agregatedCrimeRecord = crimeRecordStream
				.keyBy(CrimeRecord::getDistrict) // group by district
				.window(TumblingEventTimeWindows.of(Time.days(1)))//Time.minutes(30)
				.apply(new WindowFunction<CrimeRecord, AgregatedCrimeRecord, Integer, TimeWindow>() {

					@Override
					public void apply(Integer district, TimeWindow timeWindow, Iterable<CrimeRecord> iterable, Collector<AgregatedCrimeRecord> collector) throws Exception {

						int crimeSum = 0;
						int domesticCrimeSum = 0;
						for (CrimeRecord record : iterable) {
							crimeSum++;
							if (record.isDomestic()) domesticCrimeSum++;
						}

						long windowEnd = timeWindow.getEnd();
						Instant instantEnd = Instant.ofEpochMilli(windowEnd);
						ZonedDateTime zdtEnd = instantEnd.atZone(ZoneId.systemDefault());
						String windowEndDate = zdtEnd.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

						long windowStrart = timeWindow.getStart();
						Instant instantStart = Instant.ofEpochMilli(windowStrart);
						ZonedDateTime zdtStart = instantStart.atZone(ZoneId.systemDefault());
						String windowEndStart = zdtStart.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

						AgregatedCrimeRecord result = new AgregatedCrimeRecord(crimeSum, domesticCrimeSum, district, windowEndStart, windowEndDate);
						collector.collect(result);
					}

				});


		JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl("jdbc:postgresql://localhost:5432/crime_db")
						.withDriverName("org.postgresql.Driver")
						.withUsername("admin")
						.withPassword("admin")
						.build();

				agregatedCrimeRecord.addSink(JdbcSink.sink(
				"INSERT INTO crime_per_district_day " +
					"(total_crimes, domestic_crimes, district, window_start, window_end) "+
					"VALUES (?, ?, ?, ?, ?) " +
					"ON CONFLICT (district, window_start) DO UPDATE SET " +
					"total_crimes = EXCLUDED.total_crimes, " +
					"domestic_crimes = EXCLUDED.domestic_crimes, " +
					"window_end = EXCLUDED.window_end",

				(statement, aggregated) -> {
					statement.setInt(1, aggregated.getCrimeSum());
					statement.setInt(2, aggregated.getDomesticCrimeSum());
					statement.setInt(3, aggregated.getDistrict());

					LocalDateTime start = LocalDateTime.parse(
							aggregated.getWindowStartDate(),
							DateTimeFormatter.ISO_LOCAL_DATE_TIME
					);
					statement.setTimestamp(4, Timestamp.valueOf(start));

					LocalDateTime end = LocalDateTime.parse(
							aggregated.getWindowEndDate(),
							DateTimeFormatter.ISO_LOCAL_DATE_TIME
					);
					statement.setTimestamp(5, Timestamp.valueOf(end));

				},
				executionOptions,
				connectionOptions
		)).name("AggregatedCrimeRecord to DB");
		agregatedCrimeRecord.print();


		DataStream<AnomaliaRecord> anomaliaRecordDataStream = crimeRecordStream
				.keyBy(record -> CrimeKey.builder()
								.primaryType(record.getPrimaryType())
								.district(record.getDistrict())
								.build())
				.window(SlidingEventTimeWindows.of(
					Time.hours(5),
					Time.minutes(30)))
				.process(new AnomalyDetector(14));


		anomaliaRecordDataStream.addSink(JdbcSink.sink(
				"INSERT INTO crimes_anomaly " +
				"(total_crimes, primary_type , district , window_start , window_end ) " +
				"VALUES (?,?,?,?,?) " +
				"ON CONFLICT (primary_type, district, window_start) DO UPDATE SET " +
				"total_crimes = EXCLUDED.total_crimes, " +
				"window_end = EXCLUDED.window_end",

				((preparedStatement, anomaliaRecord) -> {
					preparedStatement.setInt(1, anomaliaRecord.getCountOfCrimes());
					preparedStatement.setString(2, anomaliaRecord.getPrimaryType());
					preparedStatement.setInt(3, anomaliaRecord.getDistrict());


					LocalDateTime start = LocalDateTime.parse(
							anomaliaRecord.getWindowStartData(),
							DateTimeFormatter.ISO_LOCAL_DATE_TIME
					);
					preparedStatement.setTimestamp(4, Timestamp.valueOf(start));


					LocalDateTime end = LocalDateTime.parse(
							anomaliaRecord.getWindowStartData(),
							DateTimeFormatter.ISO_LOCAL_DATE_TIME
					);
					preparedStatement.setTimestamp(5, Timestamp.valueOf(end));
				}),
				executionOptions,
				connectionOptions
		)).name("Anomaly to DB");
		anomaliaRecordDataStream.print();



		crimeRecordStream
				.map(crimeRecord -> {
					return new Location(crimeRecord.getLatitude(), crimeRecord.getLongitude());
				})
				.sinkTo(
						new Elasticsearch7SinkBuilder<Location>()
								.setHosts(new HttpHost("localhost", 9200, "http"))
								.setEmitter((location, context, indexer) -> {
									IndexRequest indexRequest = Requests.indexRequest()
											.index("location")
											.source(CrimeRecordSerializer.convertCrimeToJson(location), XContentType.JSON);
									indexer.add(indexRequest);
								})
								.build()
				).name("Elasticsearch Sink");


		env.execute("Flink Streaming Java API Skeleton");
	}

}
