package com.olacabs.dp.producer;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Maps;
import com.olacabs.dp.foster.models.metastore.Schema;
import com.olacabs.dp.foster.models.metastore.TopicMapping;
import com.olacabs.dp.ingestor.model.Payload;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.producer.AbstractProducer;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.row.RawJSONString;
import com.zendesk.maxwell.row.RowMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class FosterStdoutProducer extends AbstractProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(MaxwellKaasProducer.class);
	private final static ObjectMapper mapper = new ObjectMapper();
	public final static ObjectReader primaryKeyReader = mapper.readerFor(List.class);
	public final static ObjectReader columnsReader = mapper.readerFor(Map.class);
	public final static ObjectWriter objectPayloadWriter = mapper.writerFor(Payload.class);

	public FosterStdoutProducer(MaxwellContext context) {
		super(context);
	}

	private Object getData(RowMap r, String key) {
		Object val = r.getData(key);
		if (val == null)
			val = r.getData(key.toLowerCase());
		if (val == null)
			val = r.getData(key.toUpperCase());
		return val;
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

	@Override
	public void push(RowMap r) throws Exception {
		if(r==null) return;

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
				}
				else if(colValue instanceof RawJSONString){
					data.put(colName,((RawJSONString) colValue).json);
				}
				else {
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
			submitFosterPayload(context,r.getPosition(),entityId.toString(),fosterPayloadString);
			LOGGER.debug("System timestamp {}, updated_at {}, entityId {} after sending message", System.currentTimeMillis(), fosterPayload.getUpdatedAt(), entityId.toString());
		}catch (Exception e){
			LOGGER.error("{}", e);
			LOGGER.error("Error in Topic : {}, payload : {} , binlogPosition : {} ", topicMapping.getTopicName(), fosterPayload, r.getPosition());
			System.exit(1);
		}
	}

	private void submitFosterPayload(MaxwellContext context, Position position, String entityId, String fosterPayloadString) {
		LOGGER.info("Sending to Kafka: {}",fosterPayloadString);
	}
}
