package eu.scape_project.HDFSFTPServer.handlers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUploader {

	private final String HADOOP_URI;
	private final String DEST;

	public HDFSUploader(String hdpUri, String dest){
		this.HADOOP_URI = hdpUri;
		this.DEST = dest;
	}
	
	public HDFSUploader(String dest){
		this.HADOOP_URI = null;
		this.DEST = dest;
	}
	
	public void upload(String src){
		
		
		Path srcPath = new Path(src);
		Path dstPath = new Path(DEST);
		
		Configuration conf = new Configuration();
		
		if(HADOOP_URI == null){
			//Get the file system from the local Hadoop cluster.
			Path coreSitePath = new Path("/etc/hadoop", "conf/core-site.xml");
			conf.addResource(coreSitePath);
			
		} else{
			conf.set("fs.default.name", HADOOP_URI);
			
		}
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.copyFromLocalFile(srcPath, dstPath);
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
