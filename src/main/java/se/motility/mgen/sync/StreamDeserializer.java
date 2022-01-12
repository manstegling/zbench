package se.motility.mgen.sync;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import se.motility.mgen.generator.PerfMessage;
import se.motility.ziploq.api.SynchronizedConsumer;

public class StreamDeserializer {

    private final JsonFactory factory = new JsonFactory();
    private final SynchronizedConsumer<PerfMessage> consumer;

    public StreamDeserializer(SynchronizedConsumer<PerfMessage> consumer) {
        this.consumer = consumer;
    }

    public void readMessages(InputStream input) {
        PerfMessage message;
        try (JsonParser parser = factory.createParser(input)) {
            while (parser.nextToken() != null) {
                message = read(parser);
                consumer.onEvent(message, message.getT());
            }
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private PerfMessage read(JsonParser parser) throws IOException {
        PerfMessage message = new PerfMessage();

        String field;
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            field = parser.getCurrentName();
            switch (field) {
                case "t":
                    parser.nextToken();
                    message.setT(parser.getLongValue());
                    break;
                case "l":
                    parser.nextToken();
                    message.setL(parser.getLongValue());
                    break;
                case "i":
                    parser.nextToken();
                    message.setI(parser.getIntValue());
                    break;
                case "b":
                    parser.nextToken();
                    message.setB(parser.getBooleanValue());
                    break;
                case "s1":
                    parser.nextToken();
                    message.setS1(parser.getText());
                    break;
                case "s2":
                    parser.nextToken();
                    message.setS2(parser.getText());
                    break;
                case "s3":
                    parser.nextToken();
                    message.setS3(parser.getText());
                    break;
                case "s4":
                    parser.nextToken();
                    message.setS4(parser.getText());
                    break;
                default:
                    throw new IllegalArgumentException("Unknown field: " + field);
            }
        }
        return message;
    }

}
