package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.command.impl.listing.ListArgument;
import org.apache.ftpserver.command.impl.listing.ListArgumentParser;
import org.apache.ftpserver.ftplet.DataConnection;
import org.apache.ftpserver.ftplet.DataConnectionFactory;
import org.apache.ftpserver.ftplet.DefaultFtpReply;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.IODataConnectionFactory;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;
import eu.scape_project.HDFSFTPServer.handlers.DirectoryLister;
import eu.scape_project.HDFSFTPServer.handlers.FileFormatter;
import eu.scape_project.HDFSFTPServer.handlers.LISTFileFormater;

public class LIST extends AbstractCommand{
	private final Logger Log = LoggerFactory.getLogger(LIST.class);
	private DirectoryLister directoryLister = new DirectoryLister();

	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		try{
			session.resetState();
			
			ListArgument parsedArg = ListArgumentParser.parse(
					request.getArgument());
			
			//Create connection with hadoop and get OutputStream
			FileSystem fs = HadoopEnv.getFileSystem(session.getUser().getName());
			Path workDir = fs.getWorkingDirectory();
			
			fs.setWorkingDirectory(workDir);
			
			Path hdFilePath = new Path(parsedArg.getFile());
					
			
			if(!fs.exists(hdFilePath)){
				Log.debug("Listing on a non-existing file");
				session.
					write(LocalizedFtpReply.translate(session, request,context,
							FtpReply.REPLY_450_REQUESTED_FILE_ACTION_NOT_TAKEN,
							"LIST", null));
				return;
			}
			
			//check if PORT or PASV is issued, as in (FTPSERVER 110)
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
					FtpReply.REPLY_150_FILE_STATUS_OKAY, "LIST", null));
			
			
			DataConnection dataConnection;
			try{
				dataConnection = session.getDataConnection().openConnection();
				
			}catch (Exception e){
				Log.debug("Exception getting the output data stream");
				session.
					write(LocalizedFtpReply.translate(session, request,context,
							FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION,
							"LIST", null));
				return;
			}
			
			boolean failure = false;
			try {
				FileFormatter hdfsff = new LISTFileFormater();
				String ls = directoryLister.listFiles(session.getUser().
						getName(), parsedArg, hdfsff);
				Log.info(ls);
				dataConnection.transferToClient(session.getFtpletSession(), ls);
				
			}catch (SocketException ex){
				Log.debug("Socket exception during list transfer",ex);
				failure = true;
				session.
					write(LocalizedFtpReply.translate(session, request,context,
						FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
						"LIST", null));
			}catch (IOException ex){
				Log.debug("IOException during list transfer");
				failure = true;
				session.
					write(LocalizedFtpReply.translate(session, request,context,
				FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
						"LIST", null));
			}catch (IllegalArgumentException e){
				Log.debug("Illegal list syntax " + request.getArgument(), e);
				failure = true;
				session.
					write(LocalizedFtpReply.translate(session, request,context,
					FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
					"LIST", null));
			}
			
			//if data transfer OK - send transfer complete message
			if(!failure){
				session.
					write(LocalizedFtpReply.translate(session, request,context,
						FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "LIST",
						null));
			}
			
		}finally {
			session.getDataConnection().closeDataConnection();
		}
		
	}
	
	
}
