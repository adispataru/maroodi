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
import eu.scape_project.HDFSFTPServer.handlers.MLSDFileFormater;

public class MLSD extends AbstractCommand{

	private final Logger LOG = LoggerFactory.getLogger(MLSD.class);

    private DirectoryLister directoryLister = new DirectoryLister();

    /**
     * Execute command.
     */
    public void execute(final FtpIoSession session,
            final FtpServerContext context, final FtpRequest request)
            throws IOException, FtpException {


        try {

            // reset state
            session.resetState();
            
          //Create connection with hadoop and get OutputStream
            FileSystem fs = HadoopEnv.getFileSystem(session.getUser(), session);
			Path workDir = fs.getWorkingDirectory();
			
			
			
			if(!fs.exists(workDir)){
				try{
					fs.mkdirs(workDir);
				}catch (IOException e){
					LOG.debug("Cannot create directory " + workDir.toString());
					session.
						write(LocalizedFtpReply.translate(session, request,
						context, FtpReply.REPLY_451_REQUESTED_ACTION_ABORTED,
						"LIST", "Cannot access user directory"));
					return;
				}
			}
			
			fs.setWorkingDirectory(workDir);

            // 24-10-2007 - added check if PORT or PASV is issued, see
            // https://issues.apache.org/jira/browse/FTPSERVER-110
            DataConnectionFactory connFactory = session.getDataConnection();
            if (connFactory instanceof IODataConnectionFactory) {
                InetAddress address = ((IODataConnectionFactory) connFactory)
                        .getInetAddress();
                if (address == null) {
                    session.write(new DefaultFtpReply(
                            FtpReply.REPLY_503_BAD_SEQUENCE_OF_COMMANDS,
                            "PORT or PASV must be issued first"));
                    return;
                }
            }

            // get data connection
            session.write(LocalizedFtpReply.translate(session, request, context,
                    FtpReply.REPLY_150_FILE_STATUS_OKAY, "MLSD", null));

            // print listing data
            DataConnection dataConnection;
            try {
                dataConnection = session.getDataConnection().openConnection();
            } catch (Exception e) {
                LOG.debug("Exception getting the output data stream", e);
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_425_CANT_OPEN_DATA_CONNECTION, "MLSD",
                        null));
                return;
            }

            boolean failure = false;
            try {
                // parse argument
                ListArgument parsedArg = ListArgumentParser.parse(request
                        .getArgument());

                FileFormatter formater = new MLSDFileFormater((String[]) session
                        .getAttribute("MLST.types"));

                dataConnection.transferToClient(session.getFtpletSession(), 
                		directoryLister.listFiles(fs,
                        parsedArg, formater));
            } catch (SocketException ex) {
                LOG.debug("Socket exception during data transfer", ex);
                failure = true;
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_426_CONNECTION_CLOSED_TRANSFER_ABORTED,
                        "MLSD", null));
            } catch (IOException ex) {
                LOG.debug("IOException during data transfer", ex);
                failure = true;
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_551_REQUESTED_ACTION_ABORTED_PAGE_TYPE_UNKNOWN,
                                        "MLSD", null));
            } catch (IllegalArgumentException e) {
                LOG
                        .debug("Illegal listing syntax: "
                                + request.getArgument(), e);
                // if listing syntax error - send message
                session
                        .write(LocalizedFtpReply
                                .translate(
                                        session,
                                        request,
                                        context,
                                        FtpReply.REPLY_501_SYNTAX_ERROR_IN_PARAMETERS_OR_ARGUMENTS,
                                        "MLSD", null));
            }

            // if data transfer ok - send transfer complete message
            if (!failure) {
                session.write(LocalizedFtpReply.translate(session, request, context,
                        FtpReply.REPLY_226_CLOSING_DATA_CONNECTION, "MLSD",
                        null));
            }
        } finally {
            session.getDataConnection().closeDataConnection();
        }
    }

}
