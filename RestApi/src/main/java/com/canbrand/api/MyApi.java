package com.canbrand.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.canbrand.model.Msg;
import com.canbrand.model.Msg2;
import com.canbrand.model.Utility;

@Path("myresource")
public class MyApi {

	private static String SEARCH_HOST;
	private static String jsonFile;
	private static String TABLE_NAME;
	private static String FILE_SUCCESS_MESSAGE;
	private static String FILE_FAIL_MESSAGE;

	private static String FETCH_SUCCESS_MESSAGE;
	private static String FETCH_FAILED_MESSAGE;
	private static Properties prop = new Properties();
	static {
		try {
			prop.load(new FileInputStream(
					"/home/cblap15/Shahzad/eclipse-work/RestApi/src/main/resources/application.properties"));
			SEARCH_HOST = prop.getProperty("host");
			// jsonFile = prop.getProperty("jsonFile");
			FILE_SUCCESS_MESSAGE = prop.getProperty("fileSuccessMessage");
			FILE_FAIL_MESSAGE = prop.getProperty("fileFailMessage");
			TABLE_NAME = prop.getProperty("tableName");

			FETCH_SUCCESS_MESSAGE = prop.getProperty("fetchSuccessMessage");
			FETCH_FAILED_MESSAGE = prop.getProperty("fetchFailedMessage");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	// SAVING JSON DATA TO RIAK TS
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 getIt(String json) {
		Msg2 msg = new Msg2();

		RiakClient client = null;
		try {
			client = RiakClient.newClient(SEARCH_HOST);
		} catch (UnknownHostException e1) {
			msg.setSuccess("Server Error");
		}
		// String jsonData = "";
		// BufferedReader br = null;
		List<Row> rows = null;
		try {
			// String line;
			// br = new BufferedReader(new FileReader(jsonFile));
			// while ((line = br.readLine()) != null) {
			// jsonData += line + "\n";
			// }
			JSONObject obj = new JSONObject(json);
			System.out.println("memberId: " + obj.getString("memberId"));
			System.out.println("operation: " + obj.getString("operation"));
			System.out.println("deviceId: " + obj.getString("deviceId"));
			System.out.println("userName: " + obj.getString("userName"));

			JSONArray array = obj.getJSONArray("stepCount");

			long endDate = 0, startDate = 0;
			int n;
			System.out.println("Length = " + array.length());
			for (n = 0; n < array.length(); n++) {

				JSONObject object = array.getJSONObject(n);
				String startDateStr = object.getString("start");
				SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
				Date date;
				try {
					date = sdf.parse(startDateStr);
					startDate = date.getTime();
					System.out.println(startDate);
					String endDateStr = object.getString("end");
					SimpleDateFormat sdf1 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
					Date date1;
					date1 = sdf1.parse(endDateStr);
					endDate = date1.getTime();
					System.out.println(endDate);
					rows = Arrays.asList(new Row(new Cell(obj.getInt("memberId")), new Cell(obj.getString("operation")),
							new Cell(obj.getString("deviceId")), new Cell(obj.getString("userName")),
							Cell.newTimestamp(startDate), Cell.newTimestamp(endDate),
							new Cell(object.getInt("steps"))));

					System.out.println("processing....");
					System.out.println(FILE_SUCCESS_MESSAGE);
					Store storeCmd = new Store.Builder(TABLE_NAME).withRows(rows).build();
					client.execute(storeCmd);
					System.out.println();
					msg.setSuccess(FILE_SUCCESS_MESSAGE);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}

		} catch (ExecutionException e) {
			msg.setSuccess(FILE_FAIL_MESSAGE);
			e.printStackTrace();
		} catch (InterruptedException e) {
			msg.setSuccess(FILE_FAIL_MESSAGE);
			e.printStackTrace();
		} finally {
		}
		return msg;
	}

	// GETTING DATA FROM RIAK TS
	@Path("/test")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg getSteps(String json) {

		Msg msg = new Msg();
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);

			JSONObject obj = new JSONObject(json);
			System.out.println("memberId: " + obj.getString("memberId"));
			System.out.println("start: " + obj.getString("start"));
			System.out.println("end: " + obj.getString("end"));

			String id = obj.getString("memberId");
			String startDateStr = obj.getString("start");
			String endDateStr = obj.getString("end");
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();
			System.out.println(startDate);
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();
			System.out.println(endDate);
			// long Dif = endDate - startDate;
			// System.out.println(TimeUnit.DAYS.convert(Dif,TimeUnit.MILLISECONDS));
			// int days = (int) TimeUnit.DAYS.convert(Dif,
			// TimeUnit.MILLISECONDS);
			// System.out.println("days = " + days);

			String QUERY = "select SUM(steps) from " + TABLE_NAME + " where start>= " + startDate + " and start< "
					+ endDate + "  and memberId=" + id;

			// Send the query to Riak TS
			Query query = new Query.Builder(QUERY).build();
			try {
				QueryResult queryResult = client.execute(query);
				Iterator<Row> rows = queryResult.iterator();
				while (rows.hasNext()) {
					Row row = (Row) rows.next();
					Iterator<Cell> cells = row.iterator();
					int cell1 = 0;
					while (cells.hasNext()) {
						Cell cell = (Cell) cells.next();
						int cont = Integer.parseInt(Utility.getCellStringVal(cell));
						// SETTING STEPS
						msg.setSteps(cont);
						if (cont < 100) {
							System.out.println("you earned 10% discount");
						} else if (cont > 100 && cont < 200) {
							System.out.println("you earned 20% discount");
						} else if (cont > 200 && cont < 400) {
							System.out.println("30%");
						} else {
							System.out.println("40% discount");
						}
						cell1++;
					}
					System.out.println(cell1);
				}
			} catch (Exception e) {

				e.printStackTrace();
				System.out.println(e.getMessage());
			}

			client.shutdown();
			msg.setSuccess(FETCH_SUCCESS_MESSAGE);
		} catch (Exception e) {
			msg.setSuccess(FETCH_FAILED_MESSAGE);
		}
		return msg;
	}

	// SELECTING START, END, STEPS FROM RIAK TIMESERIES
	@Path("/test1")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public List<Msg> getData(String json) throws Exception {

		List<Msg> list = new ArrayList<>();

		Msg msg = new Msg();

		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);

			// #######################################################

			JSONObject obj = new JSONObject(json);
			System.out.println("start: " + obj.getString("start"));
			System.out.println("end: " + obj.getString("end"));

			String startDateStr = obj.getString("start");
			String endDateStr = obj.getString("end");
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();

			System.out.println(startDate);
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();

			System.out.println(endDate);
			// #######################################################

			// String queryText = "select start, end,steps from " + TABLE_NAME
			// + " where start>="+startDate+" and start< "+endDate;
			String queryText = "select start,end,steps from Emp16 where start>1478925791000 and start< 1478929378000 and memberId=712903230";
			System.out.println(queryText);
			// Send the query to Riak TS
			Query query = new Query.Builder(queryText).build();
			QueryResult queryResult = client.execute(query);
			Iterator<Row> rows = queryResult.iterator();
			Iterator<Cell> cells;
			int rowCount = 0;

			while (rows.hasNext()) {
				Row row = (Row) rows.next();
				cells = row.iterator();
				int cellNo = 0;
				List<Msg> list1 = null;
				while (cells.hasNext()) {
					list1 = new ArrayList<>();
					Cell cell = (Cell) cells.next();
					if (cellNo == 0) {
						System.out.println("Start Date " + Utility.getCellStringVal(cell));
						msg.setDate(Utility.getCellStringVal(cell));
						System.out.println("msg = " + Utility.getCellStringVal(cell));
					} else if (cellNo == 1) {
						msg.setEnd(Utility.getCellStringVal(cell));
						System.out.println("End Date : " + Utility.getCellStringVal(cell));
					} else if (cellNo == 2) {
						int i = Integer.parseInt(Utility.getCellStringVal(cell));
						msg.setSteps(i);
						System.out.println("Steps: " + Utility.getCellStringVal(cell));
					}
					cellNo++;
				}
				list1.add(msg);
				Iterator<Msg> iterator = list1.iterator();
				while (iterator.hasNext()) {
					System.out.println(iterator.next());

				}
				list.addAll(list1);
			}

			// cl1ient.shutdown();
		} catch (Exception e) {
			msg.setSuccess("Data is Not Available.");
		}
		return list;
	}

	// DELETING DATA FROM RIAK TIME SERIES
	@Path("/delete")
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public Msg deleteData() throws Exception {

		Msg msg = new Msg();
		try {
			RiakClient client = RiakClient.newClient(8087, SEARCH_HOST);
			// Table Emp18 id created baised on start Time , So for deleting
			// data i have to need Only start start time
			final List<Cell> keyCells = Arrays.asList(Cell.newTimestamp(1478925762000L));
			Delete delete = new Delete.Builder("Emp20", keyCells).build();
			try {
				client.execute(delete);
				msg.setSuccess("Delete opertion successful");
			} catch (Exception e) {
				System.out.println("Delete opertion failed: " + e.getMessage());
			}
			client.shutdown();
		} catch (Exception e) {
			msg.setSuccess("Can't be Deleted.");
		}
		return msg;
	}

	// Save RECORD
	@Path("/saveClient")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 saveData(String json) {
		Msg2 msg = new Msg2();
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);
			JSONObject obj = new JSONObject(json);
			System.out.println("deviceId: " + obj.getString("deviceId"));
			System.out.println("modelName: " + obj.getString("modelName"));
			System.out.println("manufacturer: " + obj.getString("manufacturer"));
			System.out.println("memberId: " + obj.getString("memberId"));
			System.out.println("OsType: " + obj.getString("OsType"));
			System.out.println("healthAccess: " + obj.getString("healthAccess"));
			System.out.println("is_health: " + obj.getString("is_health"));
			System.out.println("time: " + obj.getString("time"));
			String startDateStr = obj.getString("time");
			System.out.println(startDateStr);
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();
			System.out.println(startDate);

			List<Row> rows = Arrays
					.asList(new Row(new Cell(obj.getInt("deviceId")), new Cell(obj.getString("modelName")),
							new Cell(obj.getString("manufacturer")), new Cell(obj.getInt("memberId")),
							new Cell(obj.getString("OsType")), new Cell(obj.getString("healthAccess")),
							new Cell(obj.getString("is_health")), Cell.newTimestamp(startDate)));
			System.out.println("process....");
			System.out.println("processing....");
			Store storeCmd = new Store.Builder("Api").withRows(rows).build();
			client.execute(storeCmd);
			msg.setSuccess(FILE_SUCCESS_MESSAGE);
			System.out.println();

		} catch (Exception e) {
			msg.setSuccess(FILE_FAIL_MESSAGE);
		}
		return msg;
	}

	// ########################### GETTING DATA FOR NEW API##############

	@Path("/getData")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 getdata(String json) {
		Msg2 msg = new Msg2();
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);

			JSONObject obj = new JSONObject(json);
			System.out.println("memberId: " + obj.getString("memberId"));
			System.out.println("start: " + obj.getString("start"));
			System.out.println("end: " + obj.getString("end"));

			String id = obj.getString("memberId");

			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			String startDateStr = obj.getString("start");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();
			System.out.println("startDate = " + startDate);
			String endDateStr = obj.getString("end");
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();
			System.out.println("endDate = " + endDate);
			// long Dif = endDate - startDate;
			// System.out.println(TimeUnit.DAYS.convert(Dif,TimeUnit.MILLISECONDS));
			// int days = (int) TimeUnit.DAYS.convert(Dif,
			// TimeUnit.MILLISECONDS);
			// System.out.println("days = " + days);

			String QUERY = "select * from Api where time>=" + startDate + " and time<=" + endDate;
			if (id != null) {
				QUERY += " and memberId=" + id;
			}
			System.out.println(QUERY);
			// Send the query to Riak TS
			Query query = new Query.Builder(QUERY).build();
			try {
				QueryResult queryResult = client.execute(query);
				Iterator<Row> rows = queryResult.iterator();
				while (rows.hasNext()) {
					Row row = (Row) rows.next();
					Iterator<Cell> cells = row.iterator();
					int cell1 = 0;
					while (cells.hasNext()) {
						Cell cell = (Cell) cells.next();
						int cont = Integer.parseInt(Utility.getCellStringVal(cell));
						cell1++;
					}
					System.out.println(cell1);
				}
			} catch (Exception e) {
				msg.setSuccess(FETCH_FAILED_MESSAGE);
			}
			msg.setSuccess(FETCH_SUCCESS_MESSAGE);
			client.shutdown();
		} catch (Exception e) {
			msg.setSuccess(FETCH_FAILED_MESSAGE);
		}
		return msg;

	}

	// ################### GET DATA WITH SATART AND END TIME #################

	@Path("/getRangeData")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 getRangeData(String json) {
		Msg2 msg = new Msg2();
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);

			JSONObject obj = new JSONObject(json);
			System.out.println("start: " + obj.getString("start"));
			System.out.println("end: " + obj.getString("end"));

			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			String startDateStr = obj.getString("start");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();
			System.out.println("startDate = " + startDate);
			String endDateStr = obj.getString("end");
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();
			System.out.println("endDate = " + endDate);
			// long Dif = endDate - startDate;
			// System.out.println(TimeUnit.DAYS.convert(Dif,TimeUnit.MILLISECONDS));
			// int days = (int) TimeUnit.DAYS.convert(Dif,
			// TimeUnit.MILLISECONDS);
			// System.out.println("days = " + days);

			String QUERY = "select * from Api where time>=" + startDate + " and time<=" + endDate;
			System.out.println(QUERY);
			// Send the query to Riak TS
			Query query = new Query.Builder(QUERY).build();
			try {
				QueryResult queryResult = client.execute(query);
				Iterator<Row> rows = queryResult.iterator();
				while (rows.hasNext()) {
					Row row = (Row) rows.next();
					Iterator<Cell> cells = row.iterator();

					while (cells.hasNext()) {
						Cell cell = (Cell) cells.next();
						System.out.println(Utility.getCellStringVal(cell));

					}
				}
			} catch (Exception e) {
				msg.setSuccess(FETCH_FAILED_MESSAGE);
			}
			msg.setSuccess(FETCH_SUCCESS_MESSAGE);
			client.shutdown();
		} catch (Exception e) {
			msg.setSuccess(FETCH_FAILED_MESSAGE);
		}
		return msg;
	}

	// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
	@Path("/getRange")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Msg2 getRange() {

		Msg2 msg = new Msg2();
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

			String startDateStr = "12/11/2016 11:30:42";
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();
			System.out.println("startDate = " + startDate);

			Date dateobj = new Date();
			System.out.println(sdf.format(dateobj));

			String endDateStr = sdf.format(dateobj);
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();

			System.out.println("endDate = " + endDate);
			String QUERY = "select * from Api where time>=" + startDate + " and time<=" + endDate;
			System.out.println(QUERY);
			// Send the query to Riak TS
			Query query = new Query.Builder(QUERY).build();
			try {
				QueryResult queryResult = client.execute(query);
				Iterator<Row> rows = queryResult.iterator();
				while (rows.hasNext()) {
					Row row = (Row) rows.next();
					Iterator<Cell> cells = row.iterator();
					while (cells.hasNext()) {
						Cell cell = (Cell) cells.next();
						System.out.println(Utility.getCellStringVal(cell));
					}
				}
			} catch (Exception e) {
				msg.setSuccess(FETCH_FAILED_MESSAGE);
			}
			msg.setSuccess(FETCH_SUCCESS_MESSAGE);
			client.shutdown();
		} catch (Exception e) {
			msg.setSuccess(FETCH_FAILED_MESSAGE);
		}
		return msg;

	}

	// ################################LIST OF STEPS
	// ################################

	@Path("/listData")
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public List<Map<String, String>> listData(String json) throws Exception {
		List<Map<String, String>> prdMapList = new ArrayList<Map<String, String>>();
		Map<String, String> prdDataMap = null;
		try {
			RiakClient client = RiakClient.newClient(SEARCH_HOST);

			// #######################################################

			JSONObject obj = new JSONObject(json);
			System.out.println("start: " + obj.getString("start"));
			System.out.println("end: " + obj.getString("end"));

			String startDateStr = obj.getString("start");
			String endDateStr = obj.getString("end");
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			Date date = sdf.parse(startDateStr);
			long startDate = date.getTime();

			System.out.println(startDate);
			date = sdf.parse(endDateStr);
			long endDate = date.getTime();

			System.out.println(endDate);
			// #######################################################

			// String queryText = "select start, end,steps from " + TABLE_NAME
			// + " where start>="+startDate+" and start< "+endDate;
			String queryText = "select start,end,steps from Emp16 where start>1478925791000 and start< 1478929378000 and memberId=712903230";
			System.out.println(queryText);
			// Send the query to Riak TS
			Query query = new Query.Builder(queryText).build();
			QueryResult queryResult = client.execute(query);
			Iterator<Row> rows = queryResult.iterator();
			Iterator<Cell> cells;
			int rowCount = 0;

			while (rows.hasNext()) {
				Row row = (Row) rows.next();
				cells = row.iterator();
				int cellNo = 0;
				List<Msg> list1 = null;
				while (cells.hasNext()) {
					// list1 = new ArrayList<>();
					prdDataMap = new HashMap<String, String>();
					Cell cell = (Cell) cells.next();
					if (cellNo == 0) {
						System.out.println("Start Date " + Utility.getCellStringVal(cell));
						prdDataMap.put("Start", Utility.getCellStringVal(cell));
					} else if (cellNo == 1) {
						System.out.println("End Date : " + Utility.getCellStringVal(cell));
						prdDataMap.put("End", Utility.getCellStringVal(cell));
					} else if (cellNo == 2) {
						int i = Integer.parseInt(Utility.getCellStringVal(cell));
						prdDataMap.put("Steps", Utility.getCellStringVal(cell));
						System.out.println("Steps: " + Utility.getCellStringVal(cell));
					}
					prdMapList.add(prdDataMap);
					cellNo++;
				}
			}
			// cl1ient.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return prdMapList;
	}

}
