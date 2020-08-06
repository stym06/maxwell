package com.olacabs.dp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.olacabs.dp.exceptions.HttpFailureException;
import com.olacabs.dp.foster.models.metastore.Action;
import com.olacabs.dp.foster.models.metastore.SchemaRefresh;
import com.olacabs.dp.foster.models.metastore.SourceDB;
import com.olacabs.dp.foster.models.metastore.TopicMapping;
import com.olacabs.dp.foster.models.metastore.tenantlookup.TenantLookup;
import com.olacabs.dp.mail.MailHandler;
import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.schema.Database;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaCapturer;
import com.zendesk.maxwell.schema.Table;
import com.zendesk.maxwell.schema.columndef.ColumnDef;
import com.zendesk.maxwell.schema.columndef.IntColumnDef;
import com.zendesk.maxwell.schema.columndef.JsonColumnDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * This class is synchronizes the database schema with the schema stored in MetaLayer
 *
 * @author abhijit.singh
 * @version 1.0
 * @Date 15/06/16
 */

public class SchemaSynchronizer {
    private Map<String , com.olacabs.dp.foster.models.metastore.Schema> schemaMap;
    private Map<String , List<String>> primaryKeyMap = Maps.newConcurrentMap();
    private Object lock = new Object();
    private String metaServiceURL;
    private String metaServiceSchemaGETAPI;
    private String metaServiceSchemaPOSTAPI;
    private Map<String, Integer> emailCounter = Maps.newHashMap();
    private HttpGetClient httpGetClient = new HttpGetClient();
    private HttpPostClient httpPostClient = new HttpPostClient();
    private Schema dbSchema;
    private MaxwellContext maxwellContext;
    private KafkaTopicMapper kafkaTopicMapper;
    private Map<String, String> tenantNamespaceMap = Maps.newHashMap();
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaSynchronizer.class);
    private static DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final ObjectMapper mapper = new ObjectMapper();

    public SchemaSynchronizer(MaxwellContext maxwellContext, Schema dbSchema, KafkaTopicMapper kafkaTopicMapper) {
        this.metaServiceURL = maxwellContext.getConfig().metaServiceURL;
        this.metaServiceSchemaGETAPI = maxwellContext.getConfig().metaServiceSchemaGETAPI;
        this.metaServiceSchemaPOSTAPI = maxwellContext.getConfig().metaServiceSchemaPOSTAPI;
        this.kafkaTopicMapper = kafkaTopicMapper;
        this.dbSchema = dbSchema;
        this.maxwellContext = maxwellContext;
    }

    public void init(){
        SchemaRefreshHelper.init(this.maxwellContext.getConfig());
        fetchInstanceTenantMapping();
        fetchAllSchemaMeta();
        synchronizeSchemas();
        LOGGER.info("SchemaSynchronizer initialized successfully !!");
    }

    public void setDbSchema(Schema dbSchema) {
        this.dbSchema = dbSchema;
    }

    public void synchronizeSchemas(){

        Map<String, List<Map<String, String>>> dbEntitySchema = Maps.newLinkedHashMap();
		List<String> ignoredDatabases = new ArrayList<>();

        synchronized (lock) {
            for (Database d : dbSchema.getDatabases()) {

                if(SchemaCapturer.IGNORED_DATABASES.contains(d.getName()))
                    continue;

                String databaseName = d.getName();
                LOGGER.debug("Processing database {} ", databaseName);

                for (Table t : d.getTableList()) {
                    String tableName = t.getName();

                    LOGGER.debug("Processing table {} ", tableName);
                    List<ColumnDef> columnDefList = t.getColumnList();
                    List<Map<String, String>> columnList = Lists.newLinkedList();

                    int skippedCols = 0;
                    for (ColumnDef columnDef : columnDefList) {

                        if(maxwellContext.getConfig().schemaBlacklistConfig != null &&
                                maxwellContext.getConfig().schemaBlacklistConfig.containsKey(databaseName)){

                            if(maxwellContext.getConfig().schemaBlacklistConfig.get(databaseName).containsKey(tableName)) {

                                if (maxwellContext.getConfig().schemaBlacklistConfig.get(databaseName).get(tableName).contains(columnDef.getName())) {
                                    LOGGER.warn("Ignoring column :{} in table : {}", columnDef.getName(), tableName);
                                    skippedCols++;
                                    continue;
                                }
                            }
                        }

                        Map<String, String> columnMap = Maps.newHashMap();

                        if(columnDef instanceof IntColumnDef){
                            IntColumnDef intColumnDef = (IntColumnDef) columnDef;
                            if(intColumnDef.isSigned()){
                                columnMap.put(columnDef.getName(), columnDef.getType());
                            } else {
                                columnMap.put(columnDef.getName(), "bigint");
                            }
                        }
                        else if(columnDef instanceof JsonColumnDef){
                        	columnMap.put(columnDef.getName(),"string");
						}
                        else {
                            columnMap.put(columnDef.getName(), columnDef.getType());
                        }

                        columnList.add(columnDef.getPos() - skippedCols, columnMap);
                    }

					//Ignore syncing databases/tables whose tenants are not defined in metastore
                    if(getTenantFromNameSpace(databaseName,tableName)==null){
                    	ignoredDatabases.add(Utils.getSchemaName(databaseName,tableName));
                    	continue;
					}

                    dbEntitySchema.put(Utils.getSchemaName(databaseName, tableName), columnList);
                    primaryKeyMap.put(Utils.getSchemaName(databaseName, tableName), t.getPKList());
                }
            }
        }

        Map<String, List<Map<String, String>>> deltaSchema = Maps.newLinkedHashMap();

		LOGGER.warn("Ignoring unmapped databases: {}", ignoredDatabases.toString());

        for(Map.Entry<String, List<Map<String, String>>> schemaEntry : dbEntitySchema.entrySet()) {
        	LOGGER.info("Updating schema for database-table pair : {}",schemaEntry.toString());
            String keyFromDBSchema = schemaEntry.getKey();

            // ensure that the schema exists
            if(schemaMap.containsKey(keyFromDBSchema)){

                Map<String, String> metaSchemaMap = null;
                try {
                    metaSchemaMap = mapper.readValue(schemaMap.get(keyFromDBSchema).getColumns(),
                            new TypeReference<Map<String, String>>() {
                            });
                }  catch (IOException io){
                    LOGGER.error("{}",io);
                }


                if(metaSchemaMap != null && metaSchemaMap.size() > 0){
                    // if num of columns don't match
                    if(metaSchemaMap.size() != schemaEntry.getValue().size()){
                        LOGGER.warn("Schema for entity {} not synchronized between MetaLayer and Maxwell DB. " +
                                "Num of columns mismatched !! ", keyFromDBSchema);
                        LOGGER.debug("Schema in MetaLayer : {}", schemaMap.get(keyFromDBSchema));
                        LOGGER.debug("Schema in MaxwellDB : {}", schemaEntry.getValue());
                        deltaSchema.put(keyFromDBSchema, schemaEntry.getValue());
                    } else {
                        for(Map<String, String> columnMap : schemaEntry.getValue()){
                            // if there is a change in field name or data type
                            for(Map.Entry<String, String> entry : columnMap.entrySet()) {
                                if (!metaSchemaMap.containsKey(entry.getKey()) || !metaSchemaMap.get(
                                        entry.getKey()).equalsIgnoreCase(entry.getValue())){
                                    LOGGER.warn("Schema for entity {} not synchronized between MetaLayer and Maxwell DB. " +
                                            "Column name or type mismatch found !! ", keyFromDBSchema);
                                    LOGGER.info("Column in MaxwellDB : {} {}", entry.getKey(), entry.getValue());
                                    deltaSchema.put(keyFromDBSchema, schemaEntry.getValue());
                                }
                            }
                        }
                    }
                } else {
                    deltaSchema.put(keyFromDBSchema, schemaEntry.getValue());
                }
            } else {
                deltaSchema.put(keyFromDBSchema, schemaEntry.getValue());
                //schemaMap.put()
            }
        }

        if(deltaSchema != null && deltaSchema.size() > 0){
            LOGGER.info("Updating MetaStore with delta schema of size {} ", deltaSchema.size());
            updateMetaLayer(deltaSchema);
            LOGGER.info("Reinitializing schemaMap as there is a change in schema");
            fetchAllSchemaMeta(); // re-fetching the latest schema with schemaversion
        }
    }

	private void updateMetaLayer(Map<String, List<Map<String, String>>> deltaSchema){
        for(Map.Entry<String, List<Map<String, String>>> schemaEntry : deltaSchema.entrySet()){
            try {
				com.olacabs.dp.foster.models.metastore.Schema schema = new com.olacabs.dp.foster.models.metastore.Schema();
				String schemaName = schemaEntry.getKey();

				List<Map<String, String>> columns = schemaEntry.getValue();

				String[] entityNameArr = schemaName.split("-");
				schema.setNamespace(entityNameArr[0]);
				schema.setObjectName(entityNameArr[1]);

				String tenant = getTenantFromNameSpace(entityNameArr[0], entityNameArr[1]);

				LOGGER.info("Updating schema for {} with value {} in metalayer ", schemaEntry.getKey(), schemaEntry.getValue());

                schema.setCreatedBy(this.maxwellContext.getConfig().schemaCreatorUserName);
                schema.setOrg(maxwellContext.getConfig().org);

                String primaryKey = "";
                if(schemaMap.containsKey(schemaEntry.getKey())){
                    primaryKey = schemaMap.get(schemaEntry.getKey()).getPrimaryKey();
                } else {
                    List<String> primaryKeyList = primaryKeyMap.get(schemaEntry.getKey());
                    if (primaryKeyList != null && primaryKeyList.size() > 0) {
                        try {
                            primaryKey = mapper.writeValueAsString(primaryKeyList);
                        } catch (JsonProcessingException je) {
                            LOGGER.error("{}", je);
                        }
                    }
                }
                schema.setPrimaryKey(primaryKey);
                schema.setTenant(tenant);

                Date currDate = new Date();

                schema.setCreatedAt(currDate);
                schema.setUpdatedAt(currDate);
                schema.setSchemaType("table");
                schema.setUpdatedBy(this.maxwellContext.getConfig().schemaCreatorUserName);
                schema.setActionType(Action.RAW_ONLY);
                schema.setSourceDB(SourceDB.MYSQL);

                Map<String, String> columnsMap = Maps.newLinkedHashMap();
                for (Map<String, String> entry : columns) {
                    for (Map.Entry<String, String> columnEntry : entry.entrySet()) {
                        columnsMap.put(columnEntry.getKey(), columnEntry.getValue());
                    }
                }
                String columnsStr = new Gson().toJson(columnsMap);
                schema.setColumns(columnsStr);

                updateSchema(schema);
                updateTopic(schema);
            }catch (Exception ex){
                LOGGER.error("{}", ex);
                System.exit(1);
            }
        }
    }


    private void updateTopic(com.olacabs.dp.foster.models.metastore.Schema schema) {
        String objectName = schema.getOrg()+"."+schema.getTenant()+"."+schema.getNamespace()+"."+schema.getObjectName();
        TopicMapping topicMapping;
        int numPartitions = 2;
        Date currDate = new Date();

        if(kafkaTopicMapper.getTopicMap().containsKey(objectName)) {
            LOGGER.info("Topic {} already registered, not re-registering", objectName);
            return;
        }

        topicMapping = new TopicMapping();
        topicMapping.setCreatedAt(currDate);
        topicMapping.setUpdatedAt(currDate);
        topicMapping.setEntityName(objectName);
        String topicName = objectName;
        topicMapping.setTopicName(topicName);
        topicMapping.setNumPartitions(numPartitions);
        topicMapping.setReplicationFactor(2); // hardcoded default values
        topicMapping.setRetentionHours(14 * 24); // hardcoded default values
        LOGGER.info("Creating a new topic in MetaLayer : {} ", topicMapping);
        kafkaTopicMapper.updateTopicInMetaLayer(topicMapping);
    }

    public void updateSchema(com.olacabs.dp.foster.models.metastore.Schema schema){
        synchronized (lock){
            String schemaName = Utils.getSchemaName(schema);
            if(schemaMap.containsKey(schemaName)){
                schemaMap.put(schemaName, schema);
                LOGGER.debug("Updated Schema Object for {} , old Schema {} , and new Schema {} ", schemaName,
                        schemaMap.get(schemaName), schema);
            } else {
                schemaMap.put(schemaName, schema);
                LOGGER.debug("Added a new Schema Object for {} , Schema {} ", schemaName, schema);
            }

            String url = metaServiceURL.concat(metaServiceSchemaPOSTAPI);
            try {
                String jsonString = mapper.writeValueAsString(schema);

                LOGGER.debug("Posting schema {} in metalayer ", jsonString);
                httpPostClient.post(url, "application/json", jsonString);
            }catch (HttpFailureException ex){
                LOGGER.error("Couldn't sync the metalayer: {} with new schema {} ", url, schema);
                LOGGER.error("{}", ex);
            }catch (JsonProcessingException jex){
                LOGGER.error("Couldn't parse the schema: {}", schema);
                LOGGER.error("{}", jex);
            }
        }
    }

    public Map<String , com.olacabs.dp.foster.models.metastore.Schema> getSchemaMap(){
        return this.schemaMap;
    }

    public Map<String , List<String>> getPrimaryKeyMap(){
        return this.primaryKeyMap;
    }


    public void loadSchema(Map<String , com.olacabs.dp.foster.models.metastore.Schema> schemaMap){
        synchronized (lock){
            this.schemaMap = schemaMap;
        }
    }

    protected void fetchAllSchemaMeta() {

        Map<String, com.olacabs.dp.foster.models.metastore.Schema> schemaMap = Maps.newHashMap();

        for(Map.Entry<String, String> entry : tenantNamespaceMap.entrySet()) {

            // TODO GET TENANTS HERE
            List<com.olacabs.dp.foster.models.metastore.Schema> schemaList = null;
            String url = metaServiceURL.concat(metaServiceSchemaGETAPI.replace("object/{object_name}", "hierarchy/" + maxwellContext.getConfig().org + "/" + entry.getValue()));

            try {
                String response = httpGetClient.get(url, "application/json");

                try {
                    schemaList = mapper.readValue(response, new TypeReference<List<com.olacabs.dp.foster.models.metastore.Schema>>() {
                    });
                } catch (IOException io) {
                    LOGGER.error("{}", io);
                }

                for (com.olacabs.dp.foster.models.metastore.Schema schema : schemaList) {
                    schemaMap.put(Utils.getSchemaName(schema), schema);
                }

                LOGGER.info("SchemaMap loaded with schema fetched from Metalayer, size : {}", schemaMap.size());

            } catch (HttpFailureException e) {
                LOGGER.error("Failed to fetch Schema from Meta Layer " + e.getMessage());
                LOGGER.error("{}", e);
                System.exit(1);
            }
        } // end of tenant loop

        loadSchema(schemaMap);

        if(schemaMap == null || schemaMap.size() == 0){
            LOGGER.warn("No Schema found in metastore !!");
        }
    }


    /**
     * Fetch all instanceTenant mapping. For Each mysql cluster there could be multiple tenants.
     * Each tenant can have multiple databases(namespaces).
     */
    public void fetchInstanceTenantMapping(){

        List<TenantLookup> tenantLookups = null;

        String url = metaServiceURL.concat(metaServiceSchemaGETAPI.replace("schema/object/{object_name}",
                "tenant_lookup/all?instance_name="+ maxwellContext.getConfig().tenant));
        try{
            String response = httpGetClient.get(url, "application/json");
            try {
                tenantLookups = mapper.readValue(response, new TypeReference<List<TenantLookup>>() {
                });
            }  catch (IOException io){
                LOGGER.error("{}",io);
            }

            if(tenantLookups != null && tenantLookups.size() > 0){
                LOGGER.info("tenantLookupsList loaded with schema tenantLookups from Metalayer, size : {}", tenantLookups.size());
                for(TenantLookup tenantLookup : tenantLookups){
                    Set<String> namespaces = tenantLookup.getDbNames();
                    for(String namespace : namespaces) {
                        tenantNamespaceMap.put(namespace, tenantLookup.getTenantName());
                    }
                }
            }

            LOGGER.info("tenantNamespaceMap loaded ... size : {}", tenantNamespaceMap.size());

        } catch (HttpFailureException e) {
            LOGGER.error("Failed to fetch tenantLookups from Meta Layer " + e.getMessage());
            LOGGER.error("{}", e);
            System.exit(1);
        }
    }


    public String getTenantFromNameSpace(String namespace, String entityName) {
        if(namespace != null && !namespace.equalsIgnoreCase("")){
            if(tenantNamespaceMap.containsKey(namespace))
                return tenantNamespaceMap.get(namespace);
        }

//        LOGGER.warn("Unmapped database/namespace found '{}'", namespace);

        // TODO what if a namespace is not found
        boolean sendAlert = false;

        int count = 1;
        /*if(emailCounter.containsKey(namespace)){
            if(emailCounter.get(namespace) < 3) {
                sendAlert = true;
                count = emailCounter.get(namespace) + 1;
                emailCounter.put(namespace, count);
            }
        } else {
            sendAlert = true;
            emailCounter.put(namespace, count);
        }*/


        if(sendAlert) {
            MaxwellConfig config = maxwellContext.getConfig();
            MailHandler.getInstance(config.emailUsername,config.emailFrom, config.auth, config.emailHost,
                    config.emailPort).sendMail(maxwellContext.getConfig().emailToList,
                    "Unmapped database/namespace '" + namespace + "' found in Maxwell instance:" + this.maxwellContext
                            .getConfig().org + "-" +
                            maxwellContext.getConfig().tenant + " !!",
                    "Unmapped database/namespace '" + namespace + "' for entity '" + entityName + "'");
        }

        return null;
    }

    public SchemaRefresh checkSchemaReInitialize() {
        SchemaRefresh schemaRefresh = null;
        String url = metaServiceURL.concat(metaServiceSchemaGETAPI.replace("object/{object_name}",
                "schema_refresh/"+ maxwellContext.getConfig().instanceName));

        try{
            String response = httpGetClient.get(url, "application/json");
            try {
                if(response != null) {
                    schemaRefresh = mapper.readValue(response, new TypeReference<SchemaRefresh>() {
                    });
                }
            }  catch (IOException io){
                LOGGER.error("{}",io);
            }

        } catch (HttpFailureException e) {
            LOGGER.error("Failed to fetch Schema from Meta Layer " + e.getMessage());
            LOGGER.error("{}", e);
            System.exit(1);
        }
        LOGGER.info("Returning schemaRefresh : {} ", schemaRefresh);
        return schemaRefresh;
    }

    public void schemaInitialized(SchemaRefresh schemaRefresh) {
        synchronized (lock){
            String url = metaServiceURL.concat(metaServiceSchemaPOSTAPI).concat("/schema_refresh");
            try {
                String jsonString = mapper.writeValueAsString(schemaRefresh);

                LOGGER.debug("Posting schema refresh {} in metalayer ", jsonString);
                httpPostClient.post(url, "application/json", jsonString);
            }catch (HttpFailureException ex){
                LOGGER.error("Couldn't post the metalayer: {} with new schema refresh {} ", url, schemaRefresh);
                LOGGER.error("{}", ex);
            }catch (JsonProcessingException jex){
                LOGGER.error("Couldn't parse the schema refresh payload: {}", schemaRefresh);
                LOGGER.error("{}", jex);
            }
        }
    }

}
