package oozie;

import java.io.IOException;
import java.util.Properties;

import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.Job.Status;

public class CoordinatorOozieService {

	OozieClient oozieClient = null;  

	public CoordinatorOozieService(String url){
		oozieClient = new OozieClient(url);
	}
	
	public String submitJob(Properties workflowProperties) throws OozieClientException, IOException{
		Properties conf = oozieClient.createConfiguration(); 
		conf.putAll(workflowProperties); 
		return oozieClient.submit(conf); 
	}
	
	public String runJob(Properties workflowProperties) throws OozieClientException, IOException{ 
		Properties conf = oozieClient.createConfiguration(); 
		conf.putAll(workflowProperties); 
		return oozieClient.run(conf);
	}

	public void suspendJob(String jobId) throws OozieClientException {
		oozieClient.suspend(jobId); 
	}

	public void resumeJob(String jobId) throws OozieClientException {
		oozieClient.resume(jobId); 
	}

	public void killJob(String jobId) throws OozieClientException {
		oozieClient.kill(jobId); 
	}
	
	public Status getJobStatus(String jobID) throws OozieClientException{  
		CoordinatorJob job = oozieClient.getCoordJobInfo(jobID);
		return job.getStatus(); 
	}


}
