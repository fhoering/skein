package com.anaconda.skein;

import com.anaconda.skein.LogMessage;
import com.google.common.collect.AbstractIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogKey;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat.LogReader;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class LogClient {
  /* XXX: Unfortunately Hadoop doesn't have any nice public methods for
   * accessing logs. This class contains code pulled out of Hadoop 2.6.5, and
   * works with the *default* configuration of everything up to at least 3.0.2
   * (it may well work with later versions as well). The log handling was made
   * way more complicated in recent versions with
   * `LogAggregationFileController` and `LogAggregationFileControllerFactory`
   * java classes. These don't seem necessary to use when using the default
   * configuration, but our implementation here will likely break for other
   * configurations. I don't feel like using reflection yet, so punting on this
   * for now until someone has an issue.
   */

  public static class LogClientException extends IOException {
    public LogClientException(String msg) {
      super(msg);
    }
  }

  private static final String TMP_FILE_SUFFIX = ".tmp";

  private static Configuration conf;

  public LogClient(Configuration conf) {
    this.conf = conf;
  }

  Path getRemoteAppLogDir(ApplicationId appId, String appOwner) {
    Path root = new Path(conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR)
    );
    Path out = new Path(root, appOwner);

    String suffix = conf.get(YarnConfiguration.NM_REMOTE_APP_LOG_DIR_SUFFIX,
            YarnConfiguration.DEFAULT_NM_REMOTE_APP_LOG_DIR_SUFFIX);
    if (suffix != null && !suffix.isEmpty()) {
      out = new Path(out, suffix);
    }
    return new Path(out, appId.toString());
  }

  private static class LogData {
    String key;
    byte[] data;
  }

  private static class LogReaderAndModificationTime {
    LogReader reader;
    long modificationTime;

    void close() {
      if (reader != null)
        reader.close();
    }
  }

  private static int trimmedLength(byte[] buf) {
    // Remove trailing whitespace and convert to a string
    // Does so without an extra copy.
    int end = buf.length - 1;
    while ((end >= 0) && (buf[end] <= 32)) {
      end--;
    }
    return end + 1;
  }

  private LogData GetNextData(LogReaderAndModificationTime reader) throws IOException {
    LogKey key = new LogKey();
    DataInputStream valueStream = reader.reader.next(key);
    if (valueStream != null) {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      PrintStream printer = new PrintStream(os);
      while (true) {
        try {
          LogReader.readAContainerLogsForALogType(valueStream, printer, reader.modificationTime);
        } catch (EOFException eof) {
          break;
        }
      }
      LogData logData = new LogData();
      logData.key = key.toString();
      logData.data = os.toByteArray();
      return logData;
    }

    return null;
  }

  private LogReaderAndModificationTime getNextLogReader(RemoteIterator<FileStatus> nodeFiles) throws IOException {
    while (nodeFiles.hasNext()) {
      FileStatus thisNodeFile = nodeFiles.next();
      if (!thisNodeFile.getPath().getName().endsWith(TMP_FILE_SUFFIX)) {
        LogReader reader = new LogReader(conf, thisNodeFile.getPath());
        LogReaderAndModificationTime result = new LogReaderAndModificationTime();
        result.reader = reader;
        result.modificationTime = thisNodeFile.getModificationTime();
        return result;
      }
    }

    return null;
  }

  public Stream<LogMessage> getLogs(ApplicationId appId, String appOwner) throws IOException {
    Path remoteAppLogDir = getRemoteAppLogDir(appId, appOwner);
    RemoteIterator<FileStatus> nodeFiles;
    try {
      Path qualifiedLogDir =
              FileContext.getFileContext(conf).makeQualified(remoteAppLogDir);
      nodeFiles = FileContext.getFileContext(qualifiedLogDir.toUri(),
              conf).listStatus(remoteAppLogDir);
    } catch (FileNotFoundException fnf) {
      throw new LogClientException("Log aggregation has not completed or is not enabled.");
    }

    return Stream.generate(new Supplier<LogMessage>(){
      int chunkSize = 10;
      int offset = 0;
      LogReaderAndModificationTime reader = getNextLogReader(nodeFiles);
      LogData logData = GetNextData(reader);

      @Override
      public LogMessage get() {
        if (reader == null)
          return null;

        if (logData == null) {
          reader.close();
          return null;
        }

        try {
          int len = trimmedLength(logData.data);
          if (offset + chunkSize < len) {
            String str = new String(logData.data, offset, chunkSize);
            offset += chunkSize;
            return new LogMessage(logData.key, str);
          } else {
            String str = new String(logData.data, offset, len - offset - 1);
            LogMessage msg = new LogMessage(logData.key, str);
            logData = GetNextData(reader);
            if (logData == null) {
              if (reader != null)
                reader.close();
              reader = getNextLogReader(nodeFiles);
              if (reader == null)
                return msg;
              logData = GetNextData(reader);
            }
            offset = 0;
            return msg;
          }
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
