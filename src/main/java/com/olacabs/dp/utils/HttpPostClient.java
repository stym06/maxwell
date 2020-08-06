package com.olacabs.dp.utils;

import com.olacabs.dp.exceptions.HttpFailureException;
import com.olacabs.dp.foster.models.metastore.Schema;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;


public class HttpPostClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPostClient.class);

    public String post(String url , String contentType, Object content) throws HttpFailureException {
        DefaultHttpClient httpClient = null;
        StringBuilder responseStr = null;
        try {
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 60000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 60000);
            HttpPost postRequest = new HttpPost(url);

            StringEntity input = new StringEntity(content.toString());
            input.setContentType(contentType);
            postRequest.setEntity(input);

            HttpResponse response = httpClient.execute(postRequest);
            int responseCode = response.getStatusLine().getStatusCode();

            if (responseCode != 200 && responseCode != 201 && responseCode != 202) {
                throw new HttpFailureException("Failed : HTTP error code : "
                        + responseCode);
            }

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            String output;
            responseStr = new StringBuilder();
            while ((output = br.readLine()) != null) {
                LOGGER.debug("Response Received : {} ", output);
                responseStr.append(output);
            }


        } catch (MalformedURLException e) {
            LOGGER.error("",e);
        } catch (IOException e) {
            LOGGER.error("", e);
        }finally {
            if(httpClient != null)
                httpClient.getConnectionManager().shutdown();
        }
        return responseStr != null ? responseStr.toString() : null;
    }


    public static void main(String[] args) throws Exception {
        HttpPostClient httpPostClient = new HttpPostClient();
        Schema entitySchema = new Schema();
        entitySchema.setNamespace("DP");
        entitySchema.setObjectName("Test");
        entitySchema.setColumns("");

        String content = "{\"org\":\"ola\",\"tenant\":\"localhost\",\"namespace\":\"ola\",\"objectName\":\"test11\",\"schema_type\":\"table\",\"columns\":\"{\\\"id\\\":\\\"int\\\"}\",\"entityId\":null,\"status\":1,\"id\":null,\"createdAt\":\"1466589182862\",\"updatedAt\":\"1466589182862\",\"createdBy\":\"maxwell\",\"updatedBy\":\"maxwell\",\"primaryKey\":\"[\\\"id\\\"]\",\"schemaVersion\":\"0\"}";

        httpPostClient.post("http://metastore-server.mlbstg.corp.olacabs.com/v1/schema/", "application/json", content);

    }
}
