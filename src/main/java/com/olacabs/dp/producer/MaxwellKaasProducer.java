package com.olacabs.dp.producer;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.Maps;
import com.olacabs.dp.foster.models.metastore.Schema;
import com.olacabs.dp.foster.models.metastore.TopicMapping;
import com.olacabs.dp.ingestor.model.Payload;
import com.olacabs.dp.kaas.client.kafka.producer.impl.KaasProducerImpl;
import com.olacabs.dp.kaas.core.exception.KaasClientConfigChangeException;
import com.olacabs.dp.kaas.core.exception.KaasClientException;
import com.olacabs.dp.kaas.core.exception.KaasClientInitializationException;
import com.olacabs.dp.utils.KafkaTopicMapper;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RowMap;
import lombok.NonNull;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

class MaxwellKaasCallback implements Callback {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKaasCallback.class);
	private final MaxwellContext context;
	private final Position position;
	private final String json;
	private final String key;

	public MaxwellKaasCallback(MaxwellContext context, Position position, String key, String json) {
		this.context = context;
		this.position = position;
		this.json = json;
		this.key = key;
	}

	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		if (e != null) {
			if (e instanceof RecordTooLargeException) {
				LOGGER.error("RecordTooLargeException @ " + position + " -- " + key);
				LOGGER.error(e.getLocalizedMessage());
				LOGGER.error("Considering raising max.request.size broker-side.");
				LOGGER.error(json);
			} else {
				LOGGER.error("{}", e);
				try {
					LOGGER.error("Error in Topic : {}, payload : {} , binlogPosition : {} ", recordMetadata.topic(), json,
						recordMetadata.offset());
					if (this.context != null) {
						MaxwellConfig config = this.context.getConfig();
						if (config != null) {
							sendMail("Maxwell " + this.context.getConfig().org + "-" + this.context.getConfig().tenant + " going down....Failed to push data to Kafka !!", e.getMessage());
						}
					} else {
						LOGGER.error("Failed to push data to Kafka.Shutting down !!");
					}
				} catch (Exception ex) {
					LOGGER.error("{}", ex);
					try {
						if (this.context != null) {
							MaxwellConfig config = this.context.getConfig();
							if (config != null) {
								sendMail("Maxwell " + this.context.getConfig().org + "-" + this.context.getConfig().tenant + " going down....Failed to push data to Kafka !!", e.getMessage());
							}
						}
					} catch (Exception exc) {
						LOGGER.error("Failed to push data to Kafka.Shutting down !!. Error is {}", exc.getMessage());
					}
				}
				System.exit(1);
			}
		} else {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("->  key:" + key + ", partition:" + recordMetadata.partition() + ", offset:" + recordMetadata.offset());
				LOGGER.trace("   " + this.json);
				LOGGER.trace("   " + position);
				LOGGER.trace("");
			}
		}
	}

	private void sendMail(String subject, String key){
		//TODO: Add mail reporting
	}
}

public class MaxwellKaasProducer extends AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKaasProducer.class);
	private final static ObjectMapper mapper = new ObjectMapper();
	public final static ObjectReader primaryKeyReader = mapper.readerFor(List.class);
	public final static ObjectReader columnsReader = mapper.readerFor(Map.class);
	public final static ObjectWriter objectPayloadWriter = mapper.writerFor(Payload.class);
	private final KaasProducerImpl kaasProducer;

	public MaxwellKaasProducer(MaxwellContext context) {
		super(context);
		mapper.registerModule(new AfterburnerModule());
		kaasProducer = this.context.getKaasProducer();
	}

	private Object getData(RowMap r, String key) {
		Object val = r.getData(key);
		if (val == null)
			val = r.getData(key.toLowerCase());
		if (val == null)
			val = r.getData(key.toUpperCase());
		return val;
	}

	@Override
	public void push(RowMap r) throws Exception {
		if(r!=null && r.shouldOutput(outputConfig)) LOGGER.info(r.toJSON(outputConfig));


		//get rowmap value
		Date currDate = new Date();
		long currTime = System.currentTimeMillis();
		String tenant = schemaSynchronizer.getTenantFromNameSpace(r.getDatabase(), r.getTable());
		if (tenant == null)
			return;

		String objectName = context.getConfig().org + "." + tenant + "." + r.getDatabase() + "." + r.getTable();

		//get or set kafka topic mapping
		TopicMapping topicMapping = getTopicMappingDone(r,tenant,objectName,currDate);

		//get schema of rowmap
		Schema schema = schemaSynchronizer.getSchemaMap().get(r.getDatabase() + "-" + r.getTable());
		String primaryKey = schema.getPrimaryKey();
		String columns = schema.getColumns();

		//set entityId = primarykey+timestamp
		List<String> primaryKeys;
		StringBuilder entityId = new StringBuilder();
		String fosterPayloadString = null;
		Map<String, String> map;
		Map<String, String> data = Maps.newLinkedHashMap();

		try{

			//get entityId for the payload from all the primary keys of this table
			if(primaryKey!=null && !primaryKey.equalsIgnoreCase("")){
				primaryKeys = primaryKeyReader.readValue(primaryKey);
				for (String pk : primaryKeys) {
					if (entityId.length() == 0)
						entityId.append(getData(r, pk));
					else {
						entityId.append("-");
						entityId.append(getData(r, pk));
					}
				}
			}
			else{
				entityId.append(currTime);
			}

			//get map of the columns for setting data field in the payload
			map = columnsReader.readValue(columns);
			for (Map.Entry<String, String> columnEntry : map.entrySet()) {
				String colName = columnEntry.getKey();
				Object colValue = r.getData(colName);
				if (null == colValue) {
					data.put(colName, "");
				} else {
					data.put(colName, colValue.toString());
				}
			}

		}catch(JsonGenerationException e){
			LOGGER.error("{}", e);
		}

		//set foster payload
		Payload fosterPayload = getFosterPayload(r,tenant,topicMapping,schema,entityId,data);
		fosterPayloadString = objectPayloadWriter.writeValueAsString(fosterPayload);
		LOGGER.debug("Writing value in kafka {} ", fosterPayloadString);

		//commit offset
		this.context.setPosition(r);

		//submit foster payload
		try{
			LOGGER.debug("System timestamp {}, updated_at {}, entityId {} before sending message", System.currentTimeMillis(), fosterPayload.getUpdatedAt(), entityId.toString());
			submitFosterPayload(topicMapping.getTopicName(),context,r.getPosition(),entityId.toString(),fosterPayloadString,fosterPayload.getUpdatedAt(),currTime);
			LOGGER.debug("System timestamp {}, updated_at {}, entityId {} after sending message", System.currentTimeMillis(), fosterPayload.getUpdatedAt(), entityId.toString());
		}catch (Exception e){
			LOGGER.error("{}", e);
			LOGGER.error("Error in Topic : {}, payload : {} , binlogPosition : {} ", topicMapping.getTopicName(), fosterPayload, r.getPosition());
			sendMail("Maxwell " + this.context.getConfig().org + "-" + this.context.getConfig().tenant + " going down....Failed to push data to Kafka !!", "Current position : " + r.getPosition() + " \n" + e.getMessage());
			System.exit(1);
		}



	}

	private void sendMail(String subject, String key) {
		//TODO: Add mail reporting
	}

	private void submitFosterPayload(String topicName, MaxwellContext context, Position position, String entityId, String fosterPayloadString, long updatedAt, long ingestedAt) throws KaasClientInitializationException, KaasClientConfigChangeException, KaasClientException {
		MaxwellKaasCallback callback = new MaxwellKaasCallback(context,position,entityId,fosterPayloadString);
		long startTime = System.currentTimeMillis();
		try{
			kaasProducer.send(fosterPayloadString,topicName,entityId,callback);
		} catch (KaasClientConfigChangeException kccce) {
			LOGGER.warn("Kaas config is changed. Re-initialize kaas producer");
			sendMail("Maxwell " + this.context.getConfig().org + "-" + this.context.getConfig().tenant
					+ " Kaas config is changed. Re-initialize kaas producer",
				"Kaas config is changed. Re-initialize kaas producer." + kccce.getMessage());
			kaasProducer.initialize();
			kaasProducer.send(fosterPayloadString, topicName, entityId, callback);
		}

		long timeTaken = System.currentTimeMillis() - startTime;
		//TODO: Add Metric for time taken for writeLatency

		long timeDiff=0;
		if (updatedAt < ingestedAt)
			timeDiff = ingestedAt - updatedAt;
		//TODO: Add Metric for ingestion lag

	}

	private Payload getFosterPayload(RowMap r, String tenant, TopicMapping topicMapping, Schema schema, StringBuilder entityId, Map<String, String> data) {
		long updatedAt = r.getTimestampMillis();
		Payload payload = new Payload();
		payload.setUpdatedAt(updatedAt);
		payload.setNamespace(r.getDatabase());
		payload.setEntityName(r.getTable());
		payload.setOrg(context.getConfig().org);
		payload.setTenant(tenant);
		payload.setIngestedAt(getIngestedAtTS(topicMapping.getTopicName(), updatedAt));
		payload.setOpCode(getOpCode(r.getRowType()));
		if (r.getRowType().equalsIgnoreCase("insert"))
			payload.setCreatedAt(updatedAt);
		payload.setSchemaVersion(schema.getSchemaVersion().toString());
		payload.setEntityId(entityId.toString());
		payload.setData(data);
		return payload;
	}

	private TopicMapping getTopicMappingDone(RowMap r, String tenant, String objectName, Date currDate) {
		TopicMapping topicMapping;
		if (kafkaTopicMapper.getTopicMap().containsKey(objectName)) {
			topicMapping = kafkaTopicMapper.getTopicMap().get(objectName);
		} else {
			topicMapping = new TopicMapping();

			topicMapping.setCreatedAt(currDate);
			topicMapping.setUpdatedAt(currDate);
			topicMapping.setEntityName(objectName);
			String topicName = context.getConfig().org + "." + tenant + "." + r.getDatabase() + "." + r.getTable();
			topicMapping.setTopicName(topicName);
			topicMapping.setNumPartitions(2); // hardcoded default values
			topicMapping.setReplicationFactor(2); // hardcoded default values
			topicMapping.setRetentionHours(14 * 24); // hardcoded default values
			LOGGER.info("Creating a new topic in MetaLayer : {} ", topicMapping);
			kafkaTopicMapper.updateTopicInMetaLayer(topicMapping);
		}
		return topicMapping;
	}
}
