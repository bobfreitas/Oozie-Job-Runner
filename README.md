Oozie-Job-Runner
================

Allow Oozie Coordinator jobs to be submitted from HDFS

This will let you store your coordinator.xml and 
coordinator.properties on HDFS, as opposed to having
to submit the coordinator from local storage.  All it 
really does is to read the props from HDFS, reads them
and uses the OozieClient() to submit it.  

An example of submitting would be:

hadoop jar ./job-runner-1.0.0-bundle.jar oozie.CoordinatorClient http://bobdev:11000/oozie  hdfs://bobdev:8020//user/admin/test/coord/mytest.properties run

