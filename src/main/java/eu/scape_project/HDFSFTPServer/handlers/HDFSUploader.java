package eu.scape_project.HDFSFTPServer.handlers;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUploader {

	private final String HADOOP_HOME;
	private final String DEST;

	
	public HDFSUploader(String dest){
		
		this.HADOOP_HOME = "/etc/hadoop";
		this.DEST = dest;
		System.out.println(HADOOP_HOME);
		
	}
	
	public void upload(String src) {
		
		
		Path srcPath = new Path(src);
		Path dstPath = new Path(DEST);
		
		Configuration conf = new Configuration();
		
		//Get the file system from the local Hadoop cluster.
		Path coreSitePath = new Path(HADOOP_HOME, "conf/core-site.xml");
		//Path hdfsSitePath = new Path(HADOOP_HOME, "conf/hdfs-site.xml");
		conf.addResource(coreSitePath);
		//conf.addResource(hdfsSitePath);
		
		System.out.println(HADOOP_HOME);
		System.out.println(dstPath);
		
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
			if(fs.exists(new Path(dstPath.toString().substring(0, dstPath.toString().lastIndexOf("/"))))){
				System.out.printf("Path %s exists", DEST);
				fs.copyFromLocalFile(srcPath, dstPath);
			}else
				System.err.printf("Path %s does not exist.\n", DEST);
			
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(fs != null)
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
	}
}
