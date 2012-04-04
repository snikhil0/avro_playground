package com.telenav.serialize;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroInputFormat;
import org.json.JSONException;
import org.json.JSONObject;

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
	private int failed = 0;
	
	private DataFileWriter<GenericRecord> dataFileWriter;
	private Schema schema;
	
	public void initialize(Schema schema, DataFileWriter<GenericRecord> dataFileWriter) {
		this.schema = schema;
		this.dataFileWriter = dataFileWriter;
	}
	
	public static AdLog create(String line) throws IOException {
		
		String json = line.substring(line.indexOf("{"));
		AdLog obj = new AdLog();
		try {
		JSONObject node = new JSONObject(json);
		obj.setInternalRequestId(node.getLong("internalRequestId"));
		obj.setInternalRequestToVendorId(node.getLong("internalRequestToVendorId"));
		obj.setInternalAdId(node.getLong("internalAdId"));
		obj.setVendorCode(node.getString("vendorCode"));
		obj.setVendorAdId(node.getLong("vendorAdId"));
		obj.setBuisenessName(node.getString("businessName"));
		obj.setLat(node.getDouble("lat"));
		obj.setLat(node.getDouble("lon"));
		obj.setCity(node.getString("city"));
		obj.setCountry(node.getString("country"));
		obj.setPostalCode(node.getString("postalCode"));
		obj.setStreet(node.getString("street"));
		obj.setPoiId(node.getLong("poiId"));
		obj.setFailed(0);
		} catch (Exception e) {
			return null;
		}
		return obj;
	}
	
	
	public void serialize() throws IOException {
		GenericRecord datum = createAvroDatum(schema);
		dataFileWriter.append(datum);
		//dataFileWriter.flush();
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

	public static JSONObject createJson(String line) throws JSONException  {
		String json = line.substring(line.indexOf("{"));
		return new JSONObject(json);
	}

	public int getFailed() {
		return failed;
	}

	public void setFailed(int failed) {
		this.failed = failed;
	}
}
