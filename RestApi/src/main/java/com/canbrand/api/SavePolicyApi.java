package com.canbrand.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.Quorum;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.StoreValue.Option;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.canbrand.model.Msg2;

@Path("savePolicyDetails")
public class SavePolicyApi {

	private static String DATA;
	private static String SEARCH_HOST;
	private static String TABLE_NAME;
	private static String DATA_ID;
	private static String SUCCESS_MESSAGE;
	private static String FAIL_MESSAGE;
	private static Properties prop = new Properties();
	static {
		try {
			prop.load(new FileInputStream(
					"/home/cblap15/Shahzad/eclipse-work/RestApi/src/main/resources/application.properties"));
			DATA = prop.getProperty("Query");
			SEARCH_HOST = prop.getProperty("host");
			TABLE_NAME = prop.getProperty("table");
			DATA_ID = prop.getProperty("id");
			SUCCESS_MESSAGE = prop.getProperty("successMessage");
			FAIL_MESSAGE = prop.getProperty("failMessage");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 savePolicyDetails() {
		Msg2 msg = new Msg2();
		RiakClient client = null;
		try {
			client = RiakClient.newClient(SEARCH_HOST);
		} catch (UnknownHostException e1) {
			msg.setSuccess("Host is Not Resolved");
			e1.printStackTrace();
		}
		
		try {
			Namespace ns = new Namespace(TABLE_NAME);
			Location location = new Location(ns, DATA_ID);
			RiakObject riakObject = new RiakObject();
			riakObject.setContentType("application/json").setValue(BinaryValue.create(DATA));
			StoreValue store = new StoreValue.Builder(riakObject).withLocation(location).withOption(Option.W, new Quorum(3))
					.build();
			client.execute(store);
			System.out.println(SUCCESS_MESSAGE);
			msg.setSuccess(SUCCESS_MESSAGE);
		} catch (Exception e) {
			System.out.println(FAIL_MESSAGE);
			msg.setSuccess(FAIL_MESSAGE);
		}
		return msg;
	}
}
