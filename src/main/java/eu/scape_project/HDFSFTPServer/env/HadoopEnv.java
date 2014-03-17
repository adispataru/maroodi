package eu.scape_project.HDFSFTPServer.env;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopEnv {
	//TODO Make this more generic
	public static final String HADOOP_HOME = "/etc/hadoop";
	public static final String USER_PATH = "/user/";
	
	private static Map<String, FileSystem> fss = new HashMap<String, FileSystem>();
	
	public static FileSystem getFileSystem(User user, FtpIoSession session) throws IOException{
		
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		
		
		if(fss.containsKey(userKey)){
			return fss.get(userKey);
		}
		
		FileSystem fs = createFileSystem(user, session);
		
		
		return fs;
	}
	
	
	public static FileSystem createFileSystem(User user, FtpIoSession session)throws IOException{
		Configuration conf = new Configuration();
		
		Path coreSitePath = new Path(HADOOP_HOME, "conf/core-site.xml");
		conf.addResource(coreSitePath);
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		
		
		FileSystem fs = FileSystem.get(conf);
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		System.out.println("User key: " + userKey);
		fss.put(userKey, fs);
		fs.setWorkingDirectory(new Path(USER_PATH + user.getName()));
		return fs;
	}
	
	
	public static void removeFileSystem(User user, FtpIoSession session){
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		
		fss.remove(userKey);
	}
	
	public static Path getHomeDirectory(User user){
		return new Path(USER_PATH, user.getName());
	}
	
}
