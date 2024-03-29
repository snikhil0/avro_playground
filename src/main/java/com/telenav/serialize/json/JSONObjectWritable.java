package com.telenav.serialize.json;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.JsonNode;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONObjectWritable extends JSONObject implements Writable {

	/**
	 * Creates an empty JSONWritableObject.
	 */
	public JSONObjectWritable() {
		super();
	}

	/**
	 * Creates a JSONWritableObject with an initial value.
	 */
	public JSONObjectWritable(String s) throws JSONException {
		super(s);
	}

	public JSONObjectWritable(JSONObject log) throws JSONException {
		Iterator keys = log.keys();
		while(keys.hasNext()) {
			Object key = keys.next();
			Object value = log.get((String)key);
			put((String)key, value);
		}
			
	}

	/**
	 * Deserializes the JSON object.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		
		int cnt = in.readInt();
		byte[] buf = new byte[cnt];
		in.readFully(buf);
		String s = new String(buf);

		readJSONObject(s);
	}

	/**
	 * Deserializes a JSON object from a string representation.
	 *
	 * @param s string representation of the JSON object
	 */
	public void readJSONObject(String s) {
		// following block of code copied from JSONObject
		try {
			JSONTokener x = new JSONTokener(s);

			char c;
			String key;

			if (x.nextClean() != '{') {
				throw x.syntaxError("A JSONObject text must begin with '{'");
			}
			for (;;) {
				c = x.nextClean();
				switch (c) {
				case 0:
					throw x.syntaxError("A JSONObject text must end with '}'");
				case '}':
					return;
				default:
					x.back();
					key = x.nextValue().toString();
				}

				/*
				 * The key is followed by ':'. We will also tolerate '=' or
				 * '=>'.
				 */

				c = x.nextClean();
				if (c == '=') {
					if (x.next() != '>') {
						x.back();
					}
				} else if (c != ':') {
					throw x.syntaxError("Expected a ':' after a key");
				}
				put(key, x.nextValue());

				/*
				 * Pairs are separated by ','. We will also tolerate ';'.
				 */

				switch (x.nextClean()) {
				case ';':
				case ',':
					if (x.nextClean() == '}') {
						return;
					}
					x.back();
					break;
				case '}':
					return;
				default:
					throw x.syntaxError("Expected a ',' or '}'");
				}
			}
		} catch (JSONException e) {
			throw new RuntimeException("Error: invalid JSON!");
		}
	}

	/**
	 * Serializes this JSON object.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		byte[] buf = this.toString().getBytes();
		out.writeInt(buf.length);
		out.write(buf);
	}

	public boolean getBooleanUnchecked(String key) throws JSONException {
		return (Boolean) super.get(key);
	}

	public double getDoubleUnchecked(String key) throws JSONException {
		return (Double) super.get(key);
	}

	public int getIntUnchecked(String key) throws JSONException {
		return (Integer) super.get(key);
	}

	public long getLongUnchecked(String key) throws JSONException {
		return (Long) super.get(key);
	}

	public String getStringUnchecked(String key) throws JSONException {
		return (String) super.get(key);
	}
}
