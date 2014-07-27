package oozie;

import java.util.Properties;


public class CoordinatorClient {

	public static void main(String[] args) throws Exception {
		
		if (args.length < 3) {
			throw new Exception("Invalid parameters");
		}
		
		String url = args[0];
		CoordinatorOozieService coordinator = new CoordinatorOozieService(url);
		
		String propsFile = args[1];
		HDFSAccessor hdfsAccessor = new HDFSAccessor();
		Properties props = hdfsAccessor.readPropsFile(propsFile);
		
		String type = args[2];
		
		try {
			if (type.equals("submit")) {
				String jobId = coordinator.submitJob(props);
				System.out.println("Coordinator job submitted with id: " + jobId);
			}
			else if (type.equals("run")) {
				String jobId = coordinator.runJob(props);
				System.out.println("Coordinator job run with id: " + jobId);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

}
