package com.ge.predix.solsvc.machinedata.simulator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicNameValuePair;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.ge.predix.solsvc.restclient.impl.RestClient;

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

    @Autowired
    private RestClient restClient;
    /**
     *  -
     */
    @Scheduled(fixedDelay=3000)
    public void run()
    {
        
        try
        {
            runTest();
        }
        catch (Throwable e)
        {
            log.error("unable to run Machine DataSimulator Thread", e); //$NON-NLS-1$
        }
    }

    /**
     * @return String  Response string
     * @throws Exception -
     */
    public String runTest() throws Exception
    {
        List<JSONData> list = generateMockDataMap_RT();
        ObjectMapper mapper = new ObjectMapper();
        StringWriter writer = new StringWriter();
        
            mapper.writeValue(writer, list);
            return postData(writer.toString());
        
    }
    /**
     * @return -
     */
    List<JSONData> generateMockDataMap_RT()
    {
        String machineControllerId = this.applicationProperties.getMachineControllerId();
        List<JSONData> list = new ArrayList<JSONData>();
        JSONData data = new JSONData();
        data.setName("Compressor-2015:CompressionRatio"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(2.5, 3.0) - 1) * 65535.0 / 9.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:DischargePressure"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 23.0) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:SuctionPressure"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 0.21) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:MaximumPressure"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(22.0, 26.0) * 65535.0) / 100.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:MinimumPressure"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue(0.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:Temperature"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(65.0, 80.0) * 65535.0) / 200.0);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
        data.setUnit(machineControllerId);
        list.add(data);

        data = new JSONData();
        data.setName("Compressor-2015:Velocity"); //$NON-NLS-1$
        data.setTimestamp(getCurrentTimestamp());
        data.setValue((generateRandomUsageValue(0.0, 0.1) * 65535.0) / 0.5);
        data.setDatatype("DOUBLE"); //$NON-NLS-1$
        data.setRegister(""); //$NON-NLS-1$
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
    
    private String postData(String content)
    {   	
    	List<NameValuePair> parameters = new ArrayList<NameValuePair>();
    	parameters.add(new BasicNameValuePair("content", content));
    	parameters.add(new BasicNameValuePair("destinationId", "TimeSeries"));
    	parameters.add(new BasicNameValuePair("clientId", "TimeSeries"));   	
    	parameters.add(new BasicNameValuePair("tenantId", this.applicationProperties.getTenantId()));
    	
    	
    	EntityBuilder builder = EntityBuilder.create();
    	builder.setParameters(parameters);
    	HttpEntity reqEntity = builder.build();
    	
    	String serviceURL = this.applicationProperties.getPredixDataIngestionURL(); //$NON-NLS-1$
    	if (serviceURL == null) {
    		serviceURL = this.applicationProperties.getDiServiceURL();
    	}
        if (serviceURL != null) {
	        try(CloseableHttpResponse response = restClient.post(serviceURL, reqEntity, null, 100, 1000);)
	        {        	
	            
	            log.info("Service URL : " + serviceURL); //$NON-NLS-1$
	            log.info("Data : "+content);
	            
	            log.debug("Send Data to Ingestion Service : Response Code : " + response.getStatusLine().getStatusCode()); //$NON-NLS-1$
	            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
	            StringBuffer result = new StringBuffer();
	            String line = ""; //$NON-NLS-1$
	            while ((line = rd.readLine()) != null)
	            {
	                result.append(line);
	            }
	            log.info("Response : "+result.toString());
	            if (result.toString().startsWith("You successfully posted")) { //$NON-NLS-1$
	            	return "SUCCESS : "+result.toString(); //$NON-NLS-1$
	            }
				return "FAILED : "+result.toString(); //$NON-NLS-1$
	            
	        } catch (IOException e) {
	        	 log.error("unable to post data ", e); //$NON-NLS-1$
	             return "FAILED : "+e.getLocalizedMessage(); //$NON-NLS-1$
			}
        }else{
        	return "Dataingestion Service URL is empty.";
        }
    }
}
