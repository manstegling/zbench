package se.motility.mgen.sync;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;
import se.motility.mgen.generator.PerfMessage;

public class Deserializer {

    private final JsonFactory factory = new JsonFactory();

    public PerfMessage readMessage(String json) {

        PerfMessage message = new PerfMessage();

        try (JsonParser parser = factory.createParser(json)) {
            parser.nextToken();
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
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return message;
    }

}
