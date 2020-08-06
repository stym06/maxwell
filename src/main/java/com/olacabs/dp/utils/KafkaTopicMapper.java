package com.olacabs.dp.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.olacabs.dp.exceptions.HttpFailureException;
import com.olacabs.dp.foster.models.metastore.TopicMapping;
import com.zendesk.maxwell.MaxwellContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;


/**
 * This class is synchronizes the topic names with source database and tables
 *
 * @author abhijit.singh
 * @version 1.0
 * @Date 15/06/16
 */

public class KafkaTopicMapper {
    private Map<String , TopicMapping> topicMap;
    private Object lock = new Object();
    private String metaServiceURL;
    private String metaServiceTopicGETAPI;
    private String metaServiceTopicPOSTAPI;
    private HttpGetClient httpGetClient = new HttpGetClient();
    private HttpPostClient httpPostClient = new HttpPostClient();
    private MaxwellContext maxwellContext;
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTopicMapper.class);

    public KafkaTopicMapper(MaxwellContext maxwellContext) {
        this.maxwellContext = maxwellContext;
        this.metaServiceURL = this.maxwellContext.getConfig().metaServiceURL;
        this.metaServiceTopicGETAPI = this.maxwellContext.getConfig().metaServiceTopicAPI;
        this.metaServiceTopicPOSTAPI = this.maxwellContext.getConfig().metaServiceTopicAPI;
        init();
    }

    public void init(){
        fetchAllTopicMapping();
        LOGGER.info("KafkaTopicMapper initialized successfully !!");
    }

    public void updateTopicInMetaLayer(TopicMapping topicMapping){

        LOGGER.debug("Updating topicMapping in metalayer ", topicMapping);
        String url = metaServiceURL.concat(metaServiceTopicPOSTAPI);

        try {
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(topicMapping);

            LOGGER.debug("Posting topic {} in metalayer ", jsonString);
            httpPostClient.post(url, "application/json", jsonString);
            this.topicMap.put(topicMapping.getEntityName(), topicMapping);
        }catch (HttpFailureException ex){
            LOGGER.error("Couldn't sync the metalayer: {} with new topic {} ", url, topicMapping);
            LOGGER.error("{}", ex);
        }catch (JsonProcessingException jex){
            LOGGER.error("Couldn't parse the topicMapping: {}", topicMapping);
            LOGGER.error("{}", jex);
        }
    }


    public Map<String , TopicMapping> getTopicMap(){
        return this.topicMap;
    }

    public void loadTopics(Map<String , TopicMapping> topicMap){
        synchronized (lock){
            this.topicMap = topicMap;
        }
    }

    protected void fetchAllTopicMapping() {
        List<TopicMapping> topicMappingList = null;
      //  String url = metaServiceURL.concat(metaServiceTopicGETAPI).concat("\\/hierarchy/"+maxwellContext.getConfig().org+"\\/"+maxwellContext.getConfig().tenant);
       // TODO
        String url = metaServiceURL.concat(metaServiceTopicGETAPI);

        try{
            String response = httpGetClient.get(url, "application/json");
            try {
                ObjectMapper mapper = new ObjectMapper();
                topicMappingList = mapper.readValue(response, new TypeReference<List<TopicMapping>>(){});
            } catch (IOException io){
                LOGGER.error("{}",io);
            }

            Map<String , TopicMapping> topicMap = Maps.newHashMap();
            for (TopicMapping topicMapping : topicMappingList) {
                topicMap.put(topicMapping.getEntityName(), topicMapping);
            }
            loadTopics(topicMap);

        } catch (HttpFailureException e) {
            LOGGER.error("Failed to fetch topics from Meta Layer " + e.getMessage());
            LOGGER.error("{}", e);
            System.exit(1);
        }

        if(topicMap == null || topicMap.size() == 0){
            LOGGER.warn("No topics found in metastore !!");
        }
    }

}
