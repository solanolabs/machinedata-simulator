package com.ge.predix.solsvc.machinedata.simulator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 
 * @author predix -
 */
@Component
public class MachineDataSimulator
{
    /**
     * 
     */
    static final Logger         log = LoggerFactory.getLogger(MachineDataSimulator.class);

    /**
	 * 
	 */
    @Autowired
    ApplicationProperties applicationProperties;

    
    @Scheduled(fixedDelay=3000)
    public void run()
    {
        List<JSONData> list = generateMockDataMap_RT();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        try
        {
            mapper.writeValue(writer, list);
            postData(writer.toString());
        }
        catch (Throwable e)
        {
            log.error("unable to run Machine DataSimulator Thread", e);
        }
    }

    /**
     * @return -
     */
    @SuppressWarnings("nls")
    List<JSONData> generateMockDataMap_RT()
    {
        String machineControllerId = this.applicationProperties.getMachineControllerId();
        List<JSONData> list = new ArrayList<JSONData>();
        JSONData data = new JSONData();
        data.setName("Compressor-2015:CompressionRatio");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(2.5, 3.0) - 1) * 65535.0 / 9.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:DischargePressure");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 23.0) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:SuctionPressure");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 0.21) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:MaximumPressure");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(22.0, 26.0) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:MinimumPressure");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue(0.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:Temperature");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(65.0, 80.0) * 65535.0) / 200.0);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:Velocity");
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 0.1) * 65535.0) / 0.5);
        data.setDatatype("DOUBLE");
        data.setRegister("");
        data.setUnit(machineControllerId);
        list.add(data);

        return list;
    }

    private Timestamp getCurrentTimestamp()
    {
        java.util.Date date = new java.util.Date();
        Timestamp ts = new Timestamp(date.getTime());
        return ts;
    }

    private static double generateRandomUsageValue(double low, double high)
    {
        return low + Math.random() * (high - low);
    }
    
    private void postData(String content)
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
            HttpEntity reqEntity = MultipartEntityBuilder.create().addTextBody("content", content)
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
