package com.bennight.s3ABACProxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;












import java.util.Map.Entry;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.io.IOUtils;





import org.apache.commons.lang.CharSet;
import org.apache.hadoop.io.Text;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3ProxyServlet extends HttpServlet {
	private static final long serialVersionUID = 441088034534657919L;
	private String urlBase = null;
	private AmazonS3Client s3Client = null;
	private ZooKeeperInstance zooKeeperInstance = null;
	private String accumuloUser = null;
	private AuthenticationToken accumuloPass = null;
	private String accumuloNamespaceTable = null;
	private Region region = null;

	@Override
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		urlBase = config.getInitParameter("S3URLPattern");
		region = Region.getRegion(Regions.fromName(config.getInitParameter("Region"))); 
		s3Client = new AmazonS3Client(new ProfileCredentialsProvider());// fix later on
		s3Client.setRegion(region);
		accumuloUser = config.getInitParameter("AccumuloUser");
		accumuloPass = new PasswordToken(config.getInitParameter("AccumuloPass").getBytes());
		accumuloNamespaceTable = config.getInitParameter("AccumuloNamespaceAndTable");
		String accumuloInstance = config.getInitParameter("AccumuloInstance");
		String ZooKeepers = config.getInitParameter("ZooKeepers");
		zooKeeperInstance = new ZooKeeperInstance(accumuloInstance, ZooKeepers);
	}

	private S3Object getS3Object(String bucket, String key){
		return s3Client.getObject(new GetObjectRequest(bucket, key));	
	}
	
	private AssetMetadata getAssetMetadata(String id, String visibility) throws AccumuloException, AccumuloSecurityException, TableNotFoundException{
		Connector accumuloInstance = zooKeeperInstance.getConnector(accumuloUser, accumuloPass);
		Scanner scanner = accumuloInstance.createScanner(accumuloNamespaceTable, new Authorizations(visibility));
		scanner.setRange(Range.exact(id));
		scanner.fetchColumnFamily(new Text("Bucket"));
		scanner.fetchColumnFamily(new Text("ContentType"));
		scanner.fetchColumnFamily(new Text("ContentLength"));
		scanner.fetchColumnFamily(new Text("ContentSHA1"));
		scanner.fetchColumnFamily(new Text("ContentMD5"));
		scanner.fetchColumnFamily(new Text("ContentKey"));
		AssetMetadata am = new AssetMetadata();
		am.ID = id;
		for (Entry<Key, Value> kvp : scanner){
			switch (kvp.getKey().getColumnFamily().toString()){
				case "Bucket" :
					am.Bucket = kvp.getValue().toString();
					break;
				case "ContentType" :
					am.ContentType = kvp.getValue().toString();
					break;
				case "ContentLength" :
					am.ContentLength = Long.parseLong(kvp.getValue().toString());
					break;
				case "ContentSHA1" :
					am.ContentSHA1 = kvp.getValue().toString();
					break;
				case "ContentMD5" :
					am.ContentMD5 = kvp.getValue().toString();
					break;
				case "ContentKey" :
					am.ContentKey = kvp.getValue().toString();
					am.Visibility = kvp.getKey().getColumnVisibility().toString();
					break;
			}
			
		}
		return am;
	}
	
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
	  String id = req.getParameter("id");
	  InputStream in = null;
	  OutputStream out = null;
	  S3Object s3 = null;
	  try {
		  String visibility = "";  //Pull from spring security
		  AssetMetadata am = getAssetMetadata(id, visibility);
		  s3 = getS3Object(am.Bucket, am.ContentKey);
		  
		  if (s3.getObjectMetadata().getContentMD5() != am.ContentMD5){
			  //object isn't the same as what we think it is - ABORT!
		  }
		  if (s3.getObjectMetadata().getContentLength() != am.ContentLength){
			  //object isn't the same - ABORT!
		  }
		  
		  resp.setHeader("Content-Length", Long.toString(am.ContentLength));
		  resp.setHeader("Content-Type", am.ContentType);
		  out = resp.getOutputStream();
		  in = s3.getObjectContent();
		  IOUtils.copy(in, out);
		  out.flush();
	  } catch (Exception ex) {
		  //log or something
	  } finally {
		  IOUtils.closeQuietly(in);
		  IOUtils.closeQuietly(out);
		  IOUtils.closeQuietly(s3);
	  }
	}
}
