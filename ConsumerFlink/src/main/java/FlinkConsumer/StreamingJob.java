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

import Deserializer.JSONValueDeserializationSchema;
import Dto.CrimeRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

		DataStream<CrimeRecord> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		transactionStream.print();

		env.execute("Flink Streaming Java API Skeleton");
	}
}
