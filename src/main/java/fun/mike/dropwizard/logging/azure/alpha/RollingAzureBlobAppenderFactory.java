package fun.mike.dropwizard.azure.logging.alpha;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.encoder.LayoutWrappingEncoder;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import fun.mike.azure.logback.alpha.RollingAzureBlobAppender;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.async.AsyncLoggingEventAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;

@JsonTypeName("rolling-azure-blob")
public class RollingAzureBlobAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {

    private String appenderName = "rolling-azure-blob-appender";
    private String connectionString;
    private String containerName;
    private String baseBlobName;

    @JsonProperty
    public String getName() {
        return this.appenderName;
    }

    @JsonProperty
    public void setName(String name) {
        this.appenderName = name;
    }

    @JsonProperty
    public String getConnectionString() {
        return this.connectionString;
    }

    @JsonProperty
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    @JsonProperty
    public String getContainerName() {
        return this.containerName;
    }

    @JsonProperty
    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    @JsonProperty
    public String getBaseBlobName() {
        return this.baseBlobName;
    }

    @JsonProperty
    public void setBaseBlobName(String baseBlobName) {
        this.baseBlobName = baseBlobName;
    }

    @Override
    public Appender<ILoggingEvent> build(LoggerContext loggerContext, String s, LayoutFactory<ILoggingEvent> layoutFactory, LevelFilterFactory<ILoggingEvent> levelFilterFactory, AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {
        try {
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);

            CloudBlobClient client = storageAccount.createCloudBlobClient();

            CloudBlobContainer container = client.getContainerReference(containerName);

            final LayoutWrappingEncoder<ILoggingEvent> layoutEncoder = new LayoutWrappingEncoder<>();
            layoutEncoder.setLayout(buildLayout(loggerContext, layoutFactory));

            container.createIfNotExists();

            RollingAzureBlobAppender appender = new RollingAzureBlobAppender();
            appender.setContainer(container);
            appender.setBaseBlobName(baseBlobName);
            appender.setEncoder(layoutEncoder);
            appender.setName(appenderName);
            appender.setContext(loggerContext);
            appender.start();

            return wrapAsync(appender, asyncAppenderFactory);
        } catch (URISyntaxException ex) {
            throw new RuntimeException(ex);
        } catch (InvalidKeyException ex) {
            throw new RuntimeException(ex);
        } catch (StorageException ex) {
            throw new RuntimeException(ex);
        }
    }
}
