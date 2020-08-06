package com.olacabs.dp.utils;

import com.olacabs.dp.exceptions.HttpFailureException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HttpGetClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpGetClient.class);

    public String get(String url , String contentType) throws HttpFailureException {
        DefaultHttpClient httpClient = null;
        StringBuilder responseStr = null;
        try{
            httpClient = new DefaultHttpClient();
            httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 180000);
            httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 180000);
            HttpGet getRequest = new HttpGet(url);
            getRequest.addHeader("accept", contentType);

            LOGGER.debug("Hitting URL {} to fetch Schema from MetaLayer ", url);

            HttpResponse response = httpClient.execute(getRequest);
            if (response.getStatusLine().getStatusCode() != 200 && response.getStatusLine().getStatusCode() != 201
                    && response.getStatusLine().getStatusCode() != 202) {
                throw new HttpFailureException("Failed : HTTP error code : "
                        + response.getStatusLine().getStatusCode());
            }

            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));
            String output;
            responseStr = new StringBuilder();
            while ((output = br.readLine()) != null) {
                LOGGER.info("{}",output);
                responseStr.append(output);
            }

        } catch (ClientProtocolException e) {
            LOGGER.error("",e);
        } catch (IOException e) {
            LOGGER.error("",e);
        }finally {
            if(httpClient != null)
                httpClient.getConnectionManager().shutdown();
        }
        return responseStr.toString();
    }

    public static void main(String[] args) throws Exception {
        HttpGetClient httpGetClient = new HttpGetClient();
        System.out.println(httpGetClient.get("http://metastore-server.mlbstg.corp.olacabs.com/v1/schema/26", "application/json"));
    }

}
