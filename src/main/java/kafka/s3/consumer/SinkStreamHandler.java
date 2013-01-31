package kafka.s3.consumer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

interface SinkStreamHandler {
//  public OutputStream initStream();
  public File commitStreamReturnFile() throws IOException;
  public OutputStream rotateStream() throws IOException;
  public OutputStream getStream();
}
