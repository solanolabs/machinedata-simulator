package com.ge.predix.solsvc.machinedata.simulator;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author predix -
 */
public class PostDataToCloud
        implements Runnable
{
    private static Logger         log = LoggerFactory.getLogger(PostDataToCloud.class);
    private String                content;
    private ApplicationProperties applicationProperties;

    /**
     * @param content
     *            -
     * @param applicationProperties
     *            -
     */
    public PostDataToCloud(String content, ApplicationProperties applicationProperties)
    {
        this.content = content;
        this.applicationProperties = applicationProperties;
    }

    @SuppressWarnings("nls")
    @Override
    public void run()
    {

        HttpClient client = null;
        try
        {
            HttpClientBuilder builder = HttpClientBuilder.create();
            if ( this.applicationProperties.getDiServiceProxyHost() != null
                    && !"".equals(this.applicationProperties.getDiServiceProxyHost())
                    && this.applicationProperties.getDiServiceProxyPort() != null
                    && !"".equals(this.applicationProperties.getDiServiceProxyPort()) )
            {
                HttpHost proxy = new HttpHost(this.applicationProperties.getDiServiceProxyHost(),
                        Integer.parseInt(this.applicationProperties.getDiServiceProxyPort()));
                builder.setProxy(proxy);
            }
            client = builder.build();
            String serviceURL = null;
            if ( this.applicationProperties.getPredixDataIngestionURL() == null )
            {
                serviceURL = "http://" + this.applicationProperties.getDiServiceHost() + ":"
                        + this.applicationProperties.getDiServicePort() + "/SaveTimeSeriesData";
            }
            else
            {
                serviceURL = this.applicationProperties.getPredixDataIngestionURL() + "/SaveTimeSeriesData";

            }
            log.info("Service URL : " + serviceURL);
            HttpPost request = new HttpPost(serviceURL);
            HttpEntity reqEntity = MultipartEntityBuilder.create().addTextBody("content", this.content)
                    .addTextBody("destinationId", "TimeSeries").addTextBody("clientId", "TimeSeries")
                    .addTextBody("tenantId", this.applicationProperties.getTenantId()).build();
            request.setEntity(reqEntity);
            HttpResponse response = client.execute(request);
            log.debug("Send Data to Ingestion Service : Response Code : " + response.getStatusLine().getStatusCode());
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuffer result = new StringBuffer();
            String line = "";
            while ((line = rd.readLine()) != null)
            {
                result.append(line);
            }
            // log.info("Response : " + result.toString());

            //
        }
        catch (Throwable e)
        {
            log.error("unable to post data ", e);
        }

    }
}
