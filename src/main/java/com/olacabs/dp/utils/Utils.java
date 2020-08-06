package com.olacabs.dp.utils;

import com.olacabs.dp.foster.models.metastore.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by abhijit.singh on 13/06/16.
 */
public class Utils {

    private static String schemaName = "%s-%s";
    private static String topicName = "%s.%s.%s.%s";
    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    public static String getSchemaName(Schema entitySchema){
        return String.format(schemaName, entitySchema.getNamespace(), entitySchema.getObjectName());
    }

    public static String getSchemaName(String databaseName, String tableName){
        return String.format(schemaName, databaseName, tableName);
    }
    
    public static String[] parseTopicName(String topicName){
        try{
            String[] entity = topicName.split("\\.");
            return entity;
        }catch (Exception e){
            LOGGER.error("{}",e);
        }
        return null;
    }

    public static String getTopicKeyName(String org, String tenant, String namespace, String entityName){
        return String.format(schemaName, org, tenant, namespace, entityName);
    }


    public static void main(String[] args) {
        Schema entitySchema = new Schema();
        entitySchema.setNamespace("localhost");
        entitySchema.setObjectName("ola");
        System.out.println(getSchemaName(entitySchema));
    }
}
