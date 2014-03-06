package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.DataConnection;
import org.apache.ftpserver.ftplet.DataConnectionFactory;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.IODataConnectionFactory;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.ftpserver.util.IoUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;

public class RETR extends AbstractCommand{

	private final Logger Log = LoggerFactory.getLogger(RETR.class);
	
	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		
		try{
			String fileName = request.getArgument();
			User user = session.getUser();
			if(fileName == null){
				session.write(LocalizedFtpReply.translate(session, request,
						context, 
						FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
						"HDFSSTOR", null));
				return;
			}
			FileSystem fs = HadoopEnv.getFileSystem(user.getName());
			
			Path filePath;
			if(fileName.charAt(0) == '/'){
				filePath = new Path(HadoopEnv.getHomeDirectory(user), fileName);
			}else{
				filePath = new Path(fs.getWorkingDirectory(), fileName);
			}
			
			
			
			if(fileName.charAt(0) == '/')
				fileName = fileName.substring(1);
			
			//Create connection with hadoop and get OutputStream			
			//check file existence
			if(!fs.exists(filePath)){
				session.
					write(LocalizedFtpReply.translate(session, request,context,
							FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
							"RETR.missing", fileName));
			}
			
			//check if valid file
			if(!fs.isFile(filePath)){
				session.
				write(LocalizedFtpReply.translate(session, request,context,
						FtpReply.REPLY_550_REQUESTED_ACTION_NOT_TAKEN,
						"RETR.invalid", fileName));
			}
			
			
			//check if PORT or PASV is issued, as in 
			//https://issues.apache.org/jira/browse/FTPSERVER-110
			DataConnectionFactory connFactory = session.getDataConnection();
			if(connFactory instanceof IODataConnectionFactory){
				InetAddress address = ((IODataConnectionFactory) connFactory).
						getInetAddress();
				if(address == null){
					session.write(new DefaultFtpReply(
							FtpReply.REPLY_503_BAD_SEQUENCE_OF_COMMANDS,
							"PORT or PASV must be issued first"));
					return;
				}
			}
			
			//get data connection
			session.write(LocalizedFtpReply.translate(session, request,context,
					FtpReply.REPLY_150_FILE_STATUS_OKAY, "RETR", fileName));
			
			
			//send file data to client
			boolean failure = false;
			FSDataInputStream hdIS = null;
			DataConnection dataConnection;
			
			try {
				dataConnection = session.getDataConnection().openConnection();
			} catch (Exception e) {
				Log.debug("Exception getting output data stream", e);
				session.
					write(LocalizedFtpReply.translate(session, request,context,
							FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION,
							"RETR", null));
				return;
			}
			
			try{
				//open streams
				hdIS = fs.open(filePath);
				
				long trans = dataConnection.transferToClient(session.
						getFtpletSession(), hdIS);
				
				
				//attempt closing the hadoop input stream so that errors in
				//closing it will return an error to the client, as in
				//(FTPSERVER-119)
				if(hdIS != null){
					hdIS.close();
				}
				
				Log.info("File download {}.", fileName);
				Log.info("Donwloaded {} bytes", trans);
				
			}catch (SocketException ex){
				Log.debug("Socket exception during data transfer.", ex);
				failure = true;
				session.
					write(LocalizedFtpReply.translate(session, request,context,
						FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
						"RETR", fileName));
				
			}catch (IOException ex){
				Log.debug("IOException during data transfer", ex);
				failure = true;
				session.
					write(LocalizedFtpReply.translate(session, request,context,
					FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
					"RETR", fileName));
			}finally{
				IoUtils.close(hdIS);
			}
		
			if(!failure){
				session.
					write(LocalizedFtpReply.translate(session, request,context,
							FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "RETR",
							fileName));
			}
			
			
			
		}finally{
			session.resetState();
			session.getDataConnection().closeDataConnection();
		}
		
	}
	
}
