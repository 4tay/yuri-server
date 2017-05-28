package yuri;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mysql.cj.jdbc.MysqlDataSource;

public class DBConnection {

	Connection conn = null;
	MysqlDataSource dataSource = null;
	Statement stmt = null;

	public DBConnection() {
		
		System.out.println("attempt connection");
		dataSource = new MysqlDataSource();
		
		dataSource.setUser("######");
		dataSource.setPassword("#####");
		System.out.println("Baked in UN/PW");
		
		dataSource.setServerName("127.0.0.1");
		dataSource.setPort(3306);
		dataSource.setDatabaseName("yuri");

		try {
			conn = dataSource.getConnection();
			stmt = conn.createStatement();
			System.out.println("Connection was complete!");
		} catch (SQLException ex) {
			System.out.println("SQL ERROR: " + ex.getMessage());
		}
	}
	
	
	public String getEventsInRange(Float range, Float lng, Float lat) {
		
		Float lngCeiling = lng + range;
		Float lngFloor = lng - range;
		Float latCeiling = lat + range;
		Float latFloor = lat - range;
		try {
			System.out.println("SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
					+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
					+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			ResultSet rs = stmt.executeQuery("SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
					+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
					+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			if (rs.first()) {
				JSONObject fullOb = new JSONObject();
				JSONArray allCheckins = new JSONArray();
				try {
					JSONObject singleLocation = new JSONObject();
					singleLocation.put("locationID", rs.getInt(1));
					singleLocation.put("checkinID", rs.getInt(2));
					singleLocation.put("lat", rs.getFloat(3));
					singleLocation.put("lng", rs.getFloat(4));
					singleLocation.put("dtime", rs.getDate(5));
					singleLocation.put("hash", rs.getString(6));
					singleLocation.put("colorCode", rs.getInt(7));
					allCheckins.put(singleLocation);
				} catch (JSONException e) {
					System.out.println(e.getMessage());
				}
				System.out.println("moved to first!");
				while (rs.next()) {
					try {
						JSONObject singleLocation = new JSONObject();
						singleLocation.put("locationID", rs.getInt(1));
						singleLocation.put("checkinID", rs.getInt(2));
						singleLocation.put("lat", rs.getFloat(3));
						singleLocation.put("lng", rs.getFloat(4));
						singleLocation.put("dtime", rs.getDate(5));
						singleLocation.put("hash", rs.getString(6));
						singleLocation.put("colorCode", rs.getInt(7));
						allCheckins.put(singleLocation);
					} catch (JSONException e) {
						System.out.println(e.getMessage());
					}
				}
				try {
					fullOb.put("locations", allCheckins);
					conn.close();
					return String.valueOf(fullOb);
				} catch (JSONException ex) {
					System.out.println(ex.getMessage());
				}
			} else {
				System.out.println("didn't get anything back...");
			}
			conn.close();
		} catch (SQLException ex) {
			System.out.println("Error: " + ex.getMessage());
		}
		return "failure....";
	}
	
	public String addLocation(int checkinID, Float lat, Float lng, String hash, int colorCode) {
		try {
			System.out.println("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + checkinID + "','"
					+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
			stmt.execute("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + checkinID + "','"
					+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
			conn.close();
			return "Success!";
		} catch (SQLException ex) {
			System.out.println(ex.getMessage());
		}
		return "failure...";
	}
	
	public String updateLocation(int oldDotID, int potentialDotID, Float lat, Float lng, String hash, int colorCode) {
		
		
		Float range = (float) .02;
		Float lngCeiling = lng + range;
		Float lngFloor = lng - range;
		Float latCeiling = lat + range;
		Float latFloor = lat - range;
		try {
			System.out.println("SELECT * FROM checkin where checkinid = " + oldDotID +
					" and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			ResultSet rs = stmt.executeQuery("SELECT * FROM checkin where checkinid = " + oldDotID +
					" and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			
			if (rs.first()) {
				JSONArray allCheckins = new JSONArray();
				try {
					JSONObject singleLocation = new JSONObject();
					singleLocation.put("locationID", rs.getInt(1));
					singleLocation.put("checkinID", rs.getInt(2));
					singleLocation.put("lat", rs.getFloat(3));
					singleLocation.put("lng", rs.getFloat(4));
					singleLocation.put("dtime", rs.getDate(5));
					allCheckins.put(singleLocation);
				} catch (JSONException e) {
					System.out.println(e.getMessage());
				}
				System.out.println("moved to first!");
				while (rs.next()) {
					try {
						JSONObject singleLocation = new JSONObject();
						singleLocation.put("locationID", rs.getInt(1));
						singleLocation.put("checkinID", rs.getInt(2));
						singleLocation.put("lat", rs.getFloat(3));
						singleLocation.put("lng", rs.getFloat(4));
						singleLocation.put("dtime", rs.getDate(5));
						allCheckins.put(singleLocation);
					} catch (JSONException e) {
						System.out.println(e.getMessage());
					}
				}
				boolean movedDot = false;
				for(int i = 0; i< allCheckins.length(); i++) {
					JSONObject singleLocation = allCheckins.optJSONObject(i);
					Float selectingLat = (float) singleLocation.optDouble("lat");
					Float selectingLng = (float) singleLocation.optDouble("lng");
					 if ((selectingLat < latCeiling && selectingLat > latFloor) && (selectingLng < lngCeiling && selectingLng > lngFloor)) {
						 movedDot = true;
						 System.out.println("within time range and inside location bounds");
						 System.out.println("update checkin set lat = " + lat + ", lng = " + lng + ", dtime = NOW(), hash = '" 
						 + hash + "', colorCode = " + colorCode + " where checkinid = " + oldDotID);
						 stmt.execute("update checkin set lat = " + lat + ", lng = " + lng + ", dtime = NOW(), hash = '" 
						 + hash + "', colorCode = " + colorCode + " where checkinid = " + oldDotID);
						 return String.valueOf(oldDotID);
					 } else {
						 System.out.println("within time range, but outside of location bounds for location.");
					 }		
				}
				if (!movedDot) {
					 System.out.println("I found a dot within the time bound, but outside the range bound. Adding new, returning potentialDotID: " 
				 + String.valueOf(potentialDotID));
					 System.out.println("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + potentialDotID + "','"
								+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
						stmt.execute("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + potentialDotID + "','"
								+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
						conn.close();
						return String.valueOf(potentialDotID);
				 }
			} else {
				//nothing returned from initial select
				System.out.println("didn't get anything within range... inserting location!");
				
				System.out.println("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + potentialDotID + "','"
						+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
				stmt.execute("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('" + potentialDotID + "','"
						+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
				conn.close();
				return String.valueOf(potentialDotID);

			}
		} catch (SQLException ex) {
			System.out.println("Error: " + ex.getMessage());
		}
		System.out.println("Nothing returned from the initial select, likely a logic error if you ever see this in a log.");
		
		return String.valueOf(oldDotID);
	}
}