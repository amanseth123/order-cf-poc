import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.commons.lang3.StringUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;


public class OrderCloudFunction implements BackgroundFunction<GcsEvent> {
    private static final Logger log = Logger.getLogger(OrderCloudFunction.class.getName());

    private static final String EVENT_TYPE = "google.storage.object.finalize";
    private String getEnvironmentProperties() {
        return "mtech-wms-oms-poc";
    }
    private String getType(GcsEvent event){
        String name = event.getName();
        String type = "";
        if (name.contains("returns/inbound")){
            type = "returns";
        }else if (name.contains("orders/inbound")){
            type = "orders";
        }
        return type;
    }

    @Override
    public void accept(GcsEvent event, Context context) {
        try {
            if(!Objects.isNull(context)) {
                log.info("Received Context: " + context.toString());
            }

            // Cloud function will listen all events on the bucket. Filtering events and
            // considering only google.storage.object.finalize which will fire on new object creation

            if (shouldProcessGcsEvent(event, context)) {
                String type = getType(event);
                processGcsEvent(event, context,type);
            }

        } catch (Exception e) {
            throw new RuntimeException("Exception while executing OrderFileWatchFunction", e);
        }
    }
    boolean validateGcsEvent(String name,String type){
        return name.contains(type+"/inbound/") && !name.contains("outbound") && !name.endsWith("/") && name.endsWith(".json");
    }

    boolean shouldProcessGcsEvent(GcsEvent event, Context context) {
        boolean isValidGcsEvent = false;
        if (!Objects.isNull(context) && !Objects.isNull(event)
                && EVENT_TYPE.equalsIgnoreCase(context.eventType())) {
            String type = getType(event);
            String name = event.getName();

            if (validateGcsEvent(name,type)) {
                log.info("Processing event: " + event.toString());
                isValidGcsEvent = true;
            }else {
                log.info("Excluding event: " + event.toString());

            }
        }
        return isValidGcsEvent;
    }

    void processGcsEvent(GcsEvent event, Context context,String type) throws IOException {

        Storage storage = StorageOptions.getDefaultInstance().getService();
        String fileName = event.getName();
        BlobId blobId = BlobId.of(event.getBucket(), fileName);

        ReadableByteChannel channel = storage.reader(blobId);
        InputStream inputStream = Channels.newInputStream(channel);
        BufferedReader streamReader = new BufferedReader(new InputStreamReader(inputStream, UTF_8));

        String topicId = "";
        try {
            log.info("Reading file from:" + blobId.toString());
            for (String line = streamReader.readLine(); StringUtils.isNotEmpty(line); line = streamReader.readLine()) {

                Map<String, String> headers = new HashMap<>();
                headers.put("correlationId", context.eventId());
                headers.put("createdTimestamp", context.timestamp());

                publishMessage(headers, line,topicId);

                storage.delete(blobId);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Exception while reading file from GCS:" + fileName, e);
            throw e;
        }
    }

    void publishMessage(Map<String, String> headers, String message,String topicId) throws IOException {

        String projectId = "mtech-wms-oms-poc";

        Publisher publisher = null;

        try {
            TopicName topicName = TopicName.of(projectId, topicId);
            ByteString messageData = ByteString.copyFromUtf8(message);
            PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(messageData)
                    .putAllAttributes(headers).build();

            publisher = Publisher.newBuilder(topicName).build();
            log.info("Publishing message to:" + publisher.getTopicNameString());

            ApiFuture<String> apiFuture = publisher.publish(pubsubMessage);

            ApiFutures.addCallback(apiFuture, new ApiFutureCallback<>() {
                @Override
                public void onFailure(Throwable throwable) {
                    log.log(Level.SEVERE, "FAILURE Failed to publish message: " + message, throwable);
                }
                @Override
                public void onSuccess(String messageId) {
                    log.info("Published successfully. Message id:" + messageId);
                }
            }, MoreExecutors.directExecutor());

        } catch (Exception e) {
            log.log(Level.SEVERE, "Exception while publishing message: " + e.getMessage(), e);
            throw e;
        } finally {
            if (publisher != null) {
                publisher.shutdown();
            }
        }
    }
}


