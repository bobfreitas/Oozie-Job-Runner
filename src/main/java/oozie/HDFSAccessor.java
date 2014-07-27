package oozie;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSAccessor {

	private Configuration conf;

	public HDFSAccessor() {
		conf = new Configuration();
		String path = "/etc/hadoop";
		Path hadoopConfig = new Path(path + "/conf/core-site.xml");
		conf.addResource(hadoopConfig);
		conf.addResource(new Path(path + "/conf/hdfs-site.xml"));
		conf.addResource(new Path(path + "/conf/mapred-site.xml"));
	}
	
	public FileSystem getFileSystem() throws IOException {
		String fsURI = conf.get("fs.default.name");
		return FileSystem.get(URI.create(fsURI), conf);
	}
	
	public void writeHDFSContent(String dir, String fileName, List<String> content) throws IOException {
		FileSystem fs = getFileSystem();
		Path outDir = new Path(dir);
		FSDataOutputStream out = fs.create(new Path(outDir, fileName));
		for (String line : content){
			out.writeBytes(line);
		}
		out.close();
	}
	
	public Properties readPropsFile(String filePath) throws Exception{
		Path pt = new Path(filePath);
		FileSystem fs =  getFileSystem();
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		Properties props = new Properties();
		props.load(br);
		return props;
	}


}
