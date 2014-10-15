package eu.scape_project.HDFSFTPServer.env;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopEnv {
	//TODO Make this more generic

	private static Map<String, FileSystem> fss = new HashMap<String, FileSystem>();
	
	private static Map<String, String> env = new HashMap<String, String>();
	
	
	public static FileSystem getFileSystem(User user, FtpIoSession session) throws IOException{
		
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		
		
		if(fss.containsKey(userKey)){
			return fss.get(userKey);
		}
		
		return createFileSystem(user, session);

	}
	
	
	public static FileSystem createFileSystem(User user, FtpIoSession session)throws IOException{
		Configuration conf = new Configuration();
		
		
		System.out.println(env.get("hadoop.home"));
		System.out.println(env.get("user.path"));
		Path coreSitePath = new Path(env.get("hadoop.home"), "conf/core-site.xml");
		conf.addResource(coreSitePath);
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		conf.setBoolean("dfs.permissions", false);
		
		
		FileSystem fs = FileSystem.get(conf);
		System.out.println("debug");
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		System.out.println("User key: " + userKey);
		fss.put(userKey, fs);
		fs.setWorkingDirectory(new Path(env.get("user.path") + user.getName()));
		System.out.println("debug");
		return fs;
	}
	
	
	public static void removeFileSystem(User user, FtpIoSession session){
		String userKey = user.getName() + String.valueOf(session.
				getLoginTime().getTime());
		
		fss.remove(userKey);
	}
	
	public static Path getHomeDirectory(User user){
		return new Path(env.get("user.path"), user.getName());
	}
	
	public static void init() throws FileNotFoundException{
		Scanner in = new Scanner(new File("./conf/maroodi_env.properties"));
		while(in.hasNext()){
			String line = in.nextLine();
			String [] tokens = line.split("=");
			if(tokens.length > 1)
				env.put(tokens[0], tokens[1]);
		}
		
		
	}
	
}
