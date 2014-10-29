package com.bennight.s3ABACProxy;

public class AssetMetadata {
	
	public String Visibility = null;
	public String Bucket = null;
	public String ID = null;
	public String ContentType = null;
	public long ContentLength = -1;
	public String ContentSHA1 = null;
	public String ContentMD5 = null;
	public String ContentKey = null;
	
	public AssetMetadata(){}
	
	public AssetMetadata(String visibility, String bucket, String id, String contentType, long contentSize, String contentSHA1, String contentMD5, String contentKey){
		Visibility = visibility;
		Bucket = bucket;
		ID = id;
		ContentType = contentType;
		ContentLength = contentSize;
		ContentSHA1 = contentSHA1;
		ContentMD5 = contentMD5;
		ContentKey = contentKey;
	}
	
	

}
