package eu.scape_project.HDFSFTPServer.commands;

import java.io.IOException;

import org.apache.ftpserver.command.AbstractCommand;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.FtpReply;
import org.apache.ftpserver.ftplet.FtpRequest;
import org.apache.ftpserver.impl.FtpIoSession;
import org.apache.ftpserver.impl.FtpServerContext;
import org.apache.ftpserver.impl.LocalizedFtpReply;
import org.apache.hadoop.fs.FileSystem;

import eu.scape_project.HDFSFTPServer.env.HadoopEnv;

public class PWD extends AbstractCommand {

	@Override
	public void execute(FtpIoSession session, FtpServerContext context,
			FtpRequest request) throws IOException, FtpException {
		
		String userName = session.getUser().getName();
		
		FileSystem fs = HadoopEnv.getFileSystem(session.getUser(), session);
		
		String currDir = fs.getWorkingDirectory().toString();
		
		//user shall not see full path of hdfs system
		currDir = currDir.replace(HadoopEnv.USER_PATH + userName, "/");
		
		if(currDir.length() > 2)
			if(currDir.charAt(0) == currDir.charAt(1) && currDir.charAt(0) == '/')
				currDir = currDir.substring(1);

		session.write(LocalizedFtpReply.translate(session, request, context,
				 	FtpReply.REPLY_257_PATHNAME_CREATED, "PWD", currDir));
	}

}
