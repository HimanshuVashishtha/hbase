package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SlowSyncOpWriter extends ProtobufLogWriter {
  private static int count;
  private static final Log LOG = LogFactory.getLog(SlowSyncOpWriter.class);

  public SlowSyncOpWriter() {
    super();
  }

  @Override
  public void sync() throws IOException {
    try {
      if ((++count % 3) == 0) {
        LOG.debug("Sleeping for 6 sec..., count=" + count);
        Thread.sleep(6000);
      }
      super.sync();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    super.sync();
  }
}
