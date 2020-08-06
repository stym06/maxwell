package com.zendesk.maxwell.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Maps;
import com.olacabs.dp.ingestor.model.OpCodes;
import com.olacabs.dp.utils.KafkaTopicMapper;
import com.olacabs.dp.utils.SchemaSynchronizer;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.monitoring.MaxwellDiagnostic;
import com.zendesk.maxwell.monitoring.Metrics;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public abstract class AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProducer.class);
	protected final MaxwellContext context;
	protected final MaxwellOutputConfig outputConfig;
	protected final Counter succeededMessageCount;
	protected final Meter succeededMessageMeter;
	protected final Counter failedMessageCount;
	protected final Meter failedMessageMeter;
	protected final Timer messagePublishTimer;
	protected final Timer messageLatencyTimer;
	protected final Counter messageLatencySloViolationCount;

	//Foster customization
	protected Map<String, EntityTS> entityTSMap = Maps.newHashMap();
	protected SchemaSynchronizer schemaSynchronizer;
	protected KafkaTopicMapper kafkaTopicMapper;

	public AbstractProducer(MaxwellContext context) {
		this.context = context;
		this.outputConfig = context.getConfig().outputConfig;

		Metrics metrics = context.getMetrics();
		MetricRegistry metricRegistry = metrics.getRegistry();

		this.succeededMessageCount = metricRegistry.counter(metrics.metricName("messages", "succeeded"));
		this.succeededMessageMeter = metricRegistry.meter(metrics.metricName("messages", "succeeded", "meter"));
		this.failedMessageCount = metricRegistry.counter(metrics.metricName("messages", "failed"));
		this.failedMessageMeter = metricRegistry.meter(metrics.metricName("messages", "failed", "meter"));
		this.messagePublishTimer = metricRegistry.timer(metrics.metricName("message", "publish", "time"));
		this.messageLatencyTimer = metricRegistry.timer(metrics.metricName("message", "publish", "age"));
		this.messageLatencySloViolationCount = metricRegistry.counter(metrics.metricName("message", "publish", "age", "slo_violation"));
	}

	public void setSchemaSynchronizer(SchemaSynchronizer schemaSynchronizer) {
		this.schemaSynchronizer = schemaSynchronizer;
	}

	public void setKafkaTopicMapper(KafkaTopicMapper kafkaTopicMapper) {
		this.kafkaTopicMapper = kafkaTopicMapper;
	}

	abstract public void push(RowMap r) throws Exception;

	public StoppableTask getStoppableTask() {
		return null;
	}

	public Meter getFailedMessageMeter() {
		return this.failedMessageMeter;
	}

	public MaxwellDiagnostic getDiagnostic() {
		return null;
	}

	public char getOpCode(String opName){
		if(opName.equalsIgnoreCase(OpCodes.insert.toString()))
			return OpCodes.insert.opCode;
		else if(opName.equalsIgnoreCase(OpCodes.update.toString()))
			return OpCodes.update.opCode;
		else if(opName.equalsIgnoreCase(OpCodes.delete.toString()))
			return OpCodes.delete.opCode;
		else return OpCodes.unknown.opCode;
	}
	@Data
	class EntityTS {
		long updatedAt;
		long ingestedAt;
	}

	public long getIngestedAtTS(String topicName, long updatedAt){
		EntityTS entityTS;
		long currTimeStamp = System.currentTimeMillis();

		if(entityTSMap.containsKey(topicName)) {
			entityTS = entityTSMap.get(topicName);

			if(currTimeStamp <= entityTS.getIngestedAt() && updatedAt == entityTS.getUpdatedAt()) {
				currTimeStamp = entityTS.getIngestedAt() + 1;
				LOGGER.info("Setting currentTimeStamp to {}  for topic :{} , lastUpdatedAt : {} , and lastIngestedAt : {} "
					, currTimeStamp, topicName, entityTS.getUpdatedAt(), entityTS.getIngestedAt());
			}

		} else {
			entityTS = new EntityTS();
		}

		entityTS.setIngestedAt(currTimeStamp);
		entityTS.setUpdatedAt(updatedAt);
		entityTSMap.put(topicName, entityTS);

		return currTimeStamp;
	}


}
