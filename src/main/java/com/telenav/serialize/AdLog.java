package com.telenav.serialize;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser.Feature;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class AdLog extends AvroInputFormat<AdLog>{
	
	public long getInternalRequestId() {
		return internalRequestId;
	}

	public void setInternalRequestId(long internalRequestId) {
		this.internalRequestId = internalRequestId;
	}
	
	public long getInternalRequestToVendorId() {
		return internalRequestToVendorId;
	}

	public void setInternalRequestToVendorId(long internalRequestToVendorId) {
		this.internalRequestToVendorId = internalRequestToVendorId;
	}

	public long getInternalAdId() {
		return internalAdId;
	}

	public void setInternalAdId(long internalAdId) {
		this.internalAdId = internalAdId;
	}

	public String getVendorCode() {
		return vendorCode;
	}

	public void setVendorCode(String vendorCode) {
		this.vendorCode = vendorCode;
	}

	public long getVendorAdId() {
		return vendorAdId;
	}

	public void setVendorAdId(long vendorAdId) {
		this.vendorAdId = vendorAdId;
	}

	public String getBuisenessName() {
		return businessName;
	}

	public void setBuisenessName(String buisenessName) {
		this.businessName = buisenessName;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double d) {
		this.lat = d;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}

	public String getStreet() {
		return street;
	}

	public void setStreet(String street) {
		this.street = street;
	}

	public long getPoiId() {
		return poiId;
	}

	public void setPoiId(long poiId) {
		this.poiId = poiId;
	}
	
	public static ObjectMapper mapper = new ObjectMapper();
	
	public static void initializeMapper() {
		mapper.configure(Feature.ALLOW_SINGLE_QUOTES, true);
	}
	
	private long internalRequestId;
	private long internalRequestToVendorId;
	private long internalAdId;
	private String vendorCode;
	private long vendorAdId;
	private String businessName;
	private double lat;
	private double lon;
	private String city;
	private String state;
	private String country;
	private String postalCode;
	private String street;
	private long poiId;
	
	private DataFileWriter<GenericRecord> dataFileWriter;
	private Schema schema;
	
	public void initialize(Schema schema, DataFileWriter<GenericRecord> dataFileWriter) {
		this.schema = schema;
		this.dataFileWriter = dataFileWriter;
	}
	
	public static AdLog create(String line) throws JsonParseException, JsonMappingException, IOException {
		
		String json = line.substring(line.indexOf("{"));
		JsonNode node = mapper.readValue(json, JsonNode.class);
		AdLog obj = new AdLog();
		obj.setInternalRequestId(node.get("internalRequestId").asLong());
		obj.setInternalRequestToVendorId(node.get("internalRequestToVendorId").asLong());
		obj.setInternalAdId(node.get("internalAdId").asLong());
		obj.setVendorCode(node.get("vendorCode").asText());
		obj.setVendorAdId(node.get("vendorAdId").asLong());
		obj.setBuisenessName(node.get("businessName").asText());
		obj.setLat(node.get("lat").asDouble());
		obj.setLat(node.get("lon").asDouble());
		obj.setCity(node.get("city").asText());
		obj.setCountry(node.get("country").asText());
		obj.setCountry(node.get("postalCode").asText());
		obj.setCountry(node.get("street").asText());
		obj.setPoiId(node.get("poiId").asLong());
		return obj;
	}
	
	
	public void serialize() throws IOException {
		GenericRecord datum = createAvroDatum(schema);
		dataFileWriter.append(datum);
		dataFileWriter.flush();
	}
	
	public GenericRecord createAvroDatum(Schema schema) {
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("internalRequestId",getInternalRequestId());
		datum.put("internalRequestToVendorId", getInternalRequestToVendorId());
		datum.put("internalAdId", getInternalAdId());
		datum.put("vendorCode", getVendorCode());
		datum.put("vendorAdId", getVendorAdId());
		datum.put("buisenessName", getBuisenessName());
		datum.put("lat", getLat());
		datum.put("lon", getLon());
		datum.put("city", getCity());
		datum.put("country", getCountry());
		datum.put("postalCode", getPostalCode());
		datum.put("street", getStreet());
		datum.put("poiId", getPoiId());
		return datum;
	}

	public static JsonNode createJson(String line) throws JsonParseException, JsonMappingException, IOException {
		String json = line.substring(line.indexOf("{"));
		return mapper.readValue(json, JsonNode.class);
	}
}
