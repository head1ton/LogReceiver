package ai.practice.event;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileEventSource implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileEventSource.class.getName());

    public boolean keepRunning = true;
    private final int updateInterval;
    private final File file;
    private Long filePointer = 0L;
    private final EventHandler eventHandler;

    public FileEventSource(
        final int updateInterval,
        final File file,
        final Long filePointer,
        final EventHandler eventHandler) {
        this.updateInterval = updateInterval;
        this.file = file;
        this.filePointer = filePointer;
        this.eventHandler = eventHandler;
    }

    @Override
    public void run() {

        try {
            while (keepRunning) {
                Thread.sleep(updateInterval);

                long len = file.length();
                if (len < filePointer) {
                    logger.info("file was reset as filePointer is longer than file length");
                    filePointer = len;
                } else if (len > filePointer) {
                    readAppendAndSend();
                } else {
                    continue;
                }
            }
        } catch (InterruptedException e) {
            logger.info(e.getMessage());
        } catch (ExecutionException e) {
            logger.info(e.getMessage());
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    private void readAppendAndSend() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        raf.seek(filePointer);
        String line = null;
        while ((line = raf.readLine()) != null) {
            sendMessage(line);
        }

        filePointer = raf.getFilePointer();

    }

    private void sendMessage(final String line) throws ExecutionException, InterruptedException {
        String[] tokens = line.split(",");
        String key = tokens[0];
        StringBuffer value = new StringBuffer();

        for (int i = 1; i < tokens.length; i++) {
            if (i != (tokens.length - 1)) {
                value.append(tokens[i] + ",");
            } else {
                value.append(tokens[i]);
            }
        }

        MessageEvent messageEvent = new MessageEvent(key, value.toString());
        eventHandler.onMessage(messageEvent);
    }
}
