package com.markonrails.distri;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.markonrails.distri.exceptions.*;

public class DistriResult {
	
	public static final String HTTP_HEADER_PATTERN = 
			"HTTP/\\d+(\\.\\d+)?\\s(\\d{3})\\s([\\w ]+)\\s*([\\w-]+:\\s.*\\s?\\n)+";
	public static final String LOCATION_PATTERN = "Location:\\s(.*)\\s?\\n";
	public static final String FIELD_PATTERN = "([\\w-]+):\\s(.*)\\s?\\n";
	public static final String PARAM_PATTERN = "([\\w-]+)=([^=;\\n\\t\\r\\f]*)";

	private ArrayList<String> httpHeaders;
	private ArrayList<String> locations;
	private ArrayList<String> queryLinks;
	private HashMap<String, String> fields;
	private HashMap<String, String> params;
	private int code;
	private String message;
	private String content;
	
	public DistriResult(String curlee) throws InvalidResultException {
		this(curlee, null);
	}
	
	public DistriResult(String curlee, String query) throws InvalidResultException {
		Date startTime = new Date();
		
		this.httpHeaders = new ArrayList<String>();
		this.locations   = new ArrayList<String>();
		this.queryLinks  = new ArrayList<String>();
		this.fields = new HashMap<String, String>();
		this.params = new HashMap<String, String>();
		
		Pattern httpHeadersPattern = Pattern.compile(String.format("(%s\\s+)+",
				HTTP_HEADER_PATTERN));
		Matcher httpHeadersMatcher = httpHeadersPattern.matcher(curlee);
		if (!httpHeadersMatcher.find()) {
			throw new InvalidResultException();
		}
		String httpHeaders = httpHeadersMatcher.group(0);
		this.content = curlee.substring(httpHeadersMatcher.end());
		
		if (query != null && !query.isEmpty()) {
			Document doc = Jsoup.parse(content);
			Elements links = doc.select(query);
			for (Element link : links) {
				this.queryLinks.add(link.attr("href"));
			}
		}
		
		Pattern httpHeaderPattern = Pattern.compile(HTTP_HEADER_PATTERN);
		Matcher httpHeaderMatcher = httpHeaderPattern.matcher(httpHeaders);
		while (httpHeaderMatcher.find()) {
			String httpHeader = httpHeaderMatcher.group(0);
			this.httpHeaders.add(httpHeader);
			
			String code = httpHeaderMatcher.group(2);
			this.code = new Integer(code);
			
			this.message = httpHeaderMatcher.group(3);
			
			Pattern locationPattern = Pattern.compile(LOCATION_PATTERN);
			Matcher locationMatcher = locationPattern.matcher(httpHeader);
			while (locationMatcher.find()) {
				String location = locationMatcher.group(1);
				this.locations.add(location);
			}
		}
		
		String httpHeader = this.httpHeaders.get(this.httpHeaders.size() - 1);
		
		Pattern fieldPattern = Pattern.compile(FIELD_PATTERN);
		Matcher fieldMatcher = fieldPattern.matcher(httpHeader);
		while (fieldMatcher.find()) {
			String fieldName  = fieldMatcher.group(1);
			String fieldValue = fieldMatcher.group(2);
			this.fields.put(fieldName, fieldValue);
		}
		
		Pattern paramPattern = Pattern.compile(PARAM_PATTERN);
		Matcher paramMatcher = paramPattern.matcher(httpHeader);
		while (paramMatcher.find()) {
			String paramKey = paramMatcher.group(1);
			String paramVal = paramMatcher.group(2);
			this.params.put(paramKey, paramVal);
		}
		
		Date endTime = new Date();
		System.out.println(String.format("Constructing result takes %d ms", 
				endTime.getTime() - startTime.getTime()));
	}

	public ArrayList<String> getHttpHeaders() {
		return httpHeaders;
	}

	public ArrayList<String> getLocations() {
		return locations;
	}

	public ArrayList<String> getQueryLinks() {
		return queryLinks;
	}

	public HashMap<String, String> getFields() {
		return fields;
	}
	
	public String getField(String name) {
		return fields.get(name);
	}

	public HashMap<String, String> getParams() {
		return params;
	}
	
	public String getParam(String key) {
		return params.get(key);
	}

	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
	
	public String getContent() {
		return content;
	}
	
}
