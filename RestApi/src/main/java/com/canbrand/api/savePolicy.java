package com.canbrand.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONException;
import org.json.JSONObject;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.canbrand.model.Msg2;

@Path("policy")
public class savePolicy {

	private static String SEARCH_HOST;
	private static String FILE_SUCCESS_MESSAGE;
	private static String FILE_FAIL_MESSAGE;
	private static Properties prop = new Properties();
	static {
		try {
			prop.load(new FileInputStream(
					"/home/cblap15/Shahzad/eclipse-work/RestApi/src/main/resources/application.properties"));
			SEARCH_HOST = prop.getProperty("host");
			FILE_SUCCESS_MESSAGE = prop.getProperty("successMessage");
			FILE_FAIL_MESSAGE = prop.getProperty("failMessage");

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@POST
	@Path("/savePolicy")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Msg2 savePolicyDetails(String json) {
		Msg2 msg = new Msg2();

		RiakClient client = null;
		try {
			JSONObject obj = new JSONObject(json);
			System.out.println("memberId: " + obj.getString("memberId"));
			
			
			
			String id = obj.getString("memberId");
//			client = RiakClient.newClient("");
			
			client = RiakClient.newClient(SEARCH_HOST);
			System.out.println("Connected to localhost");
			Namespace ns = new Namespace("policyDetails");
			Location location = new Location(ns, id);
			RiakObject riakObject = new RiakObject();
			riakObject.setContentType("application/json").setValue(BinaryValue.create(json));
			StoreValue store = new StoreValue.Builder(riakObject).withLocation(location)
					.withOption(Option.W, new Quorum(3)).build();
			client.execute(store);
			System.out.println(FILE_SUCCESS_MESSAGE);
			msg.setSuccess(FILE_SUCCESS_MESSAGE);

		} catch (UnknownHostException e1) {
			msg.setSuccess(FILE_FAIL_MESSAGE);

		} catch (JSONException e) {
			e.printStackTrace();
			msg.setSuccess(FILE_FAIL_MESSAGE);
		} catch (ExecutionException e) {
			e.printStackTrace();
			msg.setSuccess(FILE_FAIL_MESSAGE);
		} catch (InterruptedException e) {
			e.printStackTrace();
			msg.setSuccess(FILE_FAIL_MESSAGE);
		}
		return msg;
	}

	@POST
	@Path("/getPolicy")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Msg2 getPolicyDetails(String json) {
		RiakClient client;
		Msg2 msg = new Msg2();
		try {
			client = RiakClient.newClient(SEARCH_HOST);
			JSONObject jsonObject = new JSONObject(json);
			System.out.println("memberId: " + jsonObject.getString("memberId"));
			String id = jsonObject.getString("memberId");
			Namespace ns = new Namespace("default", "policyDetails");
			Location location = new Location(ns, id);
			FetchValue fv = new FetchValue.Builder(location).build();
			FetchValue.Response response = client.execute(fv);
			RiakObject obj = response.getValue(RiakObject.class);
			System.out.println(obj.getValue());
			msg.setSuccess("Successfully Fetched.");
		} catch (UnknownHostException e) {
			msg.setSuccess("Host is not staarted");
			e.printStackTrace();
		} catch (ExecutionException e) {
			msg.setSuccess("Not Available.");
			e.printStackTrace();
		} catch (InterruptedException e) {
			msg.setSuccess("Not Available.");
			e.printStackTrace();
		}
		return msg;
	}
}
