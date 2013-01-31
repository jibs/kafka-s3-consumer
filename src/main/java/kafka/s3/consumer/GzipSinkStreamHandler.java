package kafka.s3.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.LoggerFactory;

public class GzipSinkStreamHandler implements SinkStreamHandler {
  private static final String FILEPREFIX = "gzipsink";
  private File tmpFile;
  private GZIPOutputStream goutStream;

  private static final org.slf4j.Logger logger = LoggerFactory
      .getLogger(App.class);

  public GzipSinkStreamHandler() throws IOException {
    super();
    this.tmpFile = createTmpFile();
    this.goutStream = getOutputStream(tmpFile);
  }

  private GZIPOutputStream getOutputStream(File tmpFile)
      throws FileNotFoundException, IOException {
    logger.debug("Creating gzip output stream for tmpFile: " + tmpFile);
    return new GZIPOutputStream(new FileOutputStream(tmpFile));
  }

  private File createTmpFile() throws IOException {
    logger.debug("Creating new tmpFile");
    return File.createTempFile(FILEPREFIX, null);
  }

  @Override
  public File commitStreamReturnFile() throws IOException {
    goutStream.close();
    return tmpFile;
  }

  @Override
  public OutputStream rotateStream() throws IOException {
    tmpFile.delete();
    tmpFile = createTmpFile();
    goutStream.finish();
    goutStream = getOutputStream(tmpFile);
    return goutStream;
  }

  @Override
  public OutputStream getStream() {
    return goutStream;
  }

}
