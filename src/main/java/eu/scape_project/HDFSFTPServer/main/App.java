package eu.scape_project.HDFSFTPServer.main;

import java.io.File;
import java.util.Scanner;

import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;


public class App{

	//TODO Extend the FtpServer and override the command for storing, to include HDFS URL and path of storage. 
	
	
	public static void main(String[] args) throws FtpException{
		FtpServerFactory serverFactory = new FtpServerFactory();
		ListenerFactory factory = new ListenerFactory();
		// set the port of the listener
		factory.setPort(2221);
		factory.setServerAddress("localhost");
		// define SSL configuration
		SslConfigurationFactory ssl = new SslConfigurationFactory();
		ssl.setKeystoreFile(new File("./res/ftpserver.jks"));
		ssl.setKeystorePassword("password");
		// set the SSL configuration for the listener
		factory.setSslConfiguration(ssl.createSslConfiguration());
		factory.setImplicitSsl(true);
		// replace the default listener
		serverFactory.addListener("default", factory.createListener());
		PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
		userManagerFactory.setFile(new File("./conf/users.properties"));
		serverFactory.setUserManager(userManagerFactory.createUserManager());
		// start the server
		FtpServer server = serverFactory.createServer();
		server.start();
		
		Scanner in = new Scanner(System.in);
		
		while(true){
			String s = in.nextLine();
			if(s.equals("stop")){
				server.stop();
				break;
			}
		}
		in.close();

	}

}