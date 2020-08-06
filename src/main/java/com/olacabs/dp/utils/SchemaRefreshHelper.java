package com.olacabs.dp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.olacabs.dp.exceptions.HttpFailureException;
import com.olacabs.dp.foster.models.metastore.SchemaRefresh;
import com.zendesk.maxwell.MaxwellConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sushil.ks on 30/11/16.
 */
public class SchemaRefreshHelper {
    private static String metaServiceURL;
    private static String instanceName;
    private static String metaServiceSchemaPOSTAPI;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRefreshHelper.class);
    private static final HttpPostClient httpPostClient = new HttpPostClient();

    public static void init(MaxwellConfig maxwellConfig){
        metaServiceURL = maxwellConfig.metaServiceURL;
        metaServiceSchemaPOSTAPI = maxwellConfig.metaServiceSchemaPOSTAPI;
        instanceName = maxwellConfig.instanceName;
    }

    public static void enableRestart(){
        String url = metaServiceURL.concat(metaServiceSchemaPOSTAPI).concat("/schema_refresh");
        SchemaRefresh schemaRefresh = SchemaRefresh.builder()
                .instanceName(instanceName)
                .restart(true)
                .build();
        try {
            String schemaRefreshJson = objectMapper.writeValueAsString(schemaRefresh);
            LOGGER.debug("Posting schema refresh {} in metalayer ", schemaRefreshJson);

            httpPostClient.post(url, "application/json", schemaRefreshJson);

            LOGGER.info("Successfully made schema to be refreshed");
        }catch (HttpFailureException ex){
            LOGGER.error("Couldn't post the metalayer: {} with new schema refresh {} ", url, schemaRefresh);
            LOGGER.error("{}", ex);
        }catch (JsonProcessingException jex){
            LOGGER.error("Couldn't parse the schema refresh payload: {}", schemaRefresh);
            LOGGER.error("{}", jex);
        }
    }
}
