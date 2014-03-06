package eu.scape_project.HDFSFTPServer.env;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopEnv {
	//TODO Make this more generic
	public static final String HADOOP_HOME = "/etc/hadoop";
	public static final String USER_PATH = "/user/";
	
	private static Map<String, FileSystem> fss = new HashMap<String, FileSystem>();
	
	public static FileSystem getFileSystem(String user) throws IOException{
		if(fss.containsKey(user)){
			return fss.get(user);
		}
		
		FileSystem fs = createFileSystem(user);
		fs.setWorkingDirectory(new Path(USER_PATH + user));
		
		return fs;
	}
	
	
	public static FileSystem createFileSystem(String user)throws IOException{
		Configuration conf = new Configuration();
		
		Path coreSitePath = new Path(HADOOP_HOME, "conf/core-site.xml");
		conf.addResource(coreSitePath);
			
		FileSystem fs = FileSystem.get(conf);
		fss.put(user, fs);
		return fs;
	}
	
	
	public static void removeFileSystem(String user){
		fss.remove(user);
	}
	
	public static Path getHomeDirectory(User user){
		return new Path(USER_PATH, user.getName());
	}
	
}
