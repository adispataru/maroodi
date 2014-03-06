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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;

public class STOR extends AbstractCommand{

	private final Logger Log = LoggerFactory.getLogger(STOR.class);
	
	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		try{
			
			User user = session.getUser();
			String fileName = request.getArgument();
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

			if(fs.exists(filePath)){
				fs.delete(filePath, true);
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
			
			session.write(LocalizedFtpReply.translate(session, request, context,
					FtpReply.REPLY_150_FILE_STATUS_OKAY, "HDFSSTOR", fileName))
						.awaitUninterruptibly(10000);
			
			
			DataConnection dataConnection;
			try{
				dataConnection = session.getDataConnection().openConnection();
			}catch (Exception e){
				Log.debug("Exeption getting the input data stream", e);
				session.write(LocalizedFtpReply.translate(session, request,
						context, FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION,
						"STOR", fileName));
				return;
			}
			
			
			
			//Create connection with hadoop and get OutputStream
			
			FSDataOutputStream hdfsOut = fs.create(filePath);
			
			//transfer data
			boolean failure = false;
			
			try{
				
				long trans = dataConnection.transferFromClient(session.getFtpletSession(), hdfsOut);
				Log.info("After transfer method terminates. " + trans + " B transfered.");
	
				//attempt to close the output stream so that errors in closing will
				//return an error to the client
				if(hdfsOut != null){
					hdfsOut.close();
				}
				
				Log.info("File uploaded {}", fileName);
			}catch (SocketException se){
				Log.debug("Socket exception during data transfer", se);
				failure = true;
				session.write(LocalizedFtpReply.translate(session, request,
					context, FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
					"STOR", fileName));
				
			}catch (IOException ioe){
				Log.debug("IOException during data transfer", ioe);
				failure = true;
				session.write(LocalizedFtpReply.translate(session, request,context,
					FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
					"STOR", fileName));
			
			}catch (Exception e){
				Log.debug("Other Exception", e);
			} finally {
				//make sure we really close the output stream
				IoUtils.close(hdfsOut);
			}
			
			//if data transfer ok - send transfer complete message
			if(!failure){
				session.write(LocalizedFtpReply.translate(session, request,context,
						FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "HDFSSTOR",
						fileName));
			}
			
		}finally{
			session.resetState();
			session.getDataConnection().closeDataConnection();
		}
		
	}

}
