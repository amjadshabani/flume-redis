package com.tango.logstash.flume.redis.sink.serializer;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by abootman on 6/25/15.
 */
public class CustomJsonSerializer implements Serializer {
	public static final Logger logger = LoggerFactory.getLogger(CustomJsonSerializer.class);

	@Override
	public byte[] serialize(Event event) throws RedisSerializerException {
		JsonObject jsonObject = new JsonObject();
		for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
			jsonObject.addProperty(entry.getKey(), entry.getValue());
		}

		// Adding the body to the object So we could deserialize it
		String bodyStr = new String(event.getBody());

//		 jsonObject.addProperty("body", bodyStr);
		//String correctedBodyStr = bodyStr.replaceAll("\\}", "");
		//Map<String, String> bodyMap = new HashMap<String, String>();
		String[] tags = bodyStr.split(",");
		for (String tag : tags) {
			String[] subTag = tag.split(":");
			//bodyMap.put(subTag[0].replaceAll("\\{}", ""), subTag[1]);
			jsonObject.addProperty(subTag[0].replaceAll("\\{}", ""), subTag[1].replaceAll("\\{}", ""));
			logger.debug(tag);
		}
		
		//String strEvent = gsonBody.toJson(bodyStr);
		//String correctedEvent = strEvent.replaceAll("\\","" ).replaceAll("\\\\", "");

		//jsonObject.addProperty("body", bodyStr.replaceAll("\\\\", ""));
		return jsonObject.toString().replaceAll("\\{}", "").getBytes();
	}

	/*
	 * private String decodeAvroBinary(byte[] binaryAvro) throws IOException {
	 * String str = null; BinaryDecoder decoder =
	 * DecoderFactory.get().binaryDecoder(binaryAvro, null); // str =
	 * decoder.readString(); // str = decoder.readBytes(null).array().; byte[]
	 * decoded; if (decoder.readBytes(null).hasArray()) { decoded =
	 * decoder.readBytes(null).array(); str = new String(decoded); } return str;
	 * }
	 * 
	 */
	@Override
	public void configure(Context context) {
	}

	@Override
	public void configure(ComponentConfiguration componentConfiguration) {
	}
}
