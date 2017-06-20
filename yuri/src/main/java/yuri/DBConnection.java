package yuri;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mysql.cj.jdbc.MysqlDataSource;

public class DBConnection {

	Connection conn = null;
	MysqlDataSource dataSource = null;
	Statement stmt = null;

	public DBConnection() {
		
		System.out.println(LocalDateTime.now() + ": " + "attempt connection");
		dataSource = new MysqlDataSource();
		
		dataSource.setUser("#####");
		dataSource.setPassword("######");
		System.out.println(LocalDateTime.now() + ": " + "Baked in UN/PW");
		
		dataSource.setServerName("127.0.0.1");
		dataSource.setPort(3306);
		dataSource.setDatabaseName("yuri");

		try {
			conn = dataSource.getConnection();
			stmt = conn.createStatement();
			System.out.println(LocalDateTime.now() + ": " + "Connection was complete!");
		} catch (SQLException ex) {
			System.out.println(LocalDateTime.now() + ": " + "SQL ERROR: " + ex.getMessage());
		}
	}
	
	
	public String getEventsInRange(Float range, Float lng, Float lat, String hash) {
		
		Float lngCeiling = lng + range;
		Float lngFloor = lng - range;
		Float latCeiling = lat + range;
		Float latFloor = lat - range;
		try {
			ResultSet rs = null;
			if(hash == null || hash.equalsIgnoreCase("")) {
				System.out.println(LocalDateTime.now() + ": " + "SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
					+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
					+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE) order by lat,lng;");
				rs = stmt.executeQuery("SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
					+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
					+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE) order by lat,lng;");
			} else {
				System.out.println(LocalDateTime.now() + ": " + "SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
						+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
						+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE) and"
								+ " hash like '%" + hash + "%' order by lat,lng;");
				rs = stmt.executeQuery("SELECT * FROM checkin where (lat between " + lat + " and " + latCeiling
						+ " or lat between " + latFloor + " and " + lat + ") and (lng between " + lng + " and " + lngCeiling
						+ " or lng between " + lngFloor + " and " + lng + ") and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE) and"
								+ " hash like '%" + hash + "%' order by lat,lng;");
				
			}
			if (rs != null && rs.first()) {
				JSONObject fullOb = new JSONObject();
				JSONArray allCheckins = new JSONArray();
				System.out.println(LocalDateTime.now() + ": " + "moved to first!");
				try {
					do {
						JSONObject singleLocation = new JSONObject();
						singleLocation.put("locationID", rs.getInt(1));
						singleLocation.put("checkinID", rs.getInt(2));
						singleLocation.put("lat", rs.getFloat(3));
						singleLocation.put("lng", rs.getFloat(4));
						singleLocation.put("dtime", rs.getDate(5));
						singleLocation.put("hash", rs.getString(6));
						singleLocation.put("colorCode", rs.getInt(7));
						allCheckins.put(singleLocation);
					} while(rs.next());
				} catch (JSONException e) {
					System.out.println(LocalDateTime.now() + ": " + e.getMessage());
				}
				try {
					fullOb.put("locations", allCheckins);
					conn.close();
					return String.valueOf(fullOb);
				} catch (JSONException ex) {
					System.out.println(LocalDateTime.now() + ": " + ex.getMessage());
				}
			} else {
				System.out.println(LocalDateTime.now() + ": " + "didn't get anything back...");
			}
			conn.close();
		} catch (SQLException ex) {
			System.out.println(LocalDateTime.now() + ": " + "Error: " + ex.getMessage());
		}
		return "failure....";
	}
	
	
	public String addLocation(int checkinID, Float lat, Float lng, String hash, int colorCode) {
		try {
			System.out.println(LocalDateTime.now() + ": " + "INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
					+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
			PreparedStatement statement = conn.prepareStatement("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
					+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");",Statement.RETURN_GENERATED_KEYS);
			int affectedRows = 120000;
			affectedRows = statement.executeUpdate();
			System.out.println(LocalDateTime.now() + ": " + String.valueOf(affectedRows));
			if (affectedRows == 0) {
	            throw new SQLException("Creating location failed, no rows affected.");
	        }
			try {
				ResultSet generatedKeys = statement.getGeneratedKeys();
				if (generatedKeys.next()) {
					Long rowID = generatedKeys.getLong(1);
	            	conn.close();
					return String.valueOf(rowID);
	            }
	            else {
	                throw new SQLException("Creating location failed, no ID obtained.");
	            }
	        } catch(SQLException ex) {
	        	System.out.println(LocalDateTime.now() + ": " + "Error: " + ex.getMessage());
	        }
		} catch (SQLException ex) {
			System.out.println(LocalDateTime.now() + ": " + ex.getMessage());
		}
		return "failure...";
	}
	public String putLocation(int oldDotID, Float lat, Float lng, String hash, int colorCode) {
		Float range = (float) .02;
		Float lngCeiling = lng + range;
		Float lngFloor = lng - range;
		Float latCeiling = lat + range;
		Float latFloor = lat - range;
		try {
			System.out.println(LocalDateTime.now() + ": " + "SELECT * FROM checkin where id = " + oldDotID +
					" and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			ResultSet rs = stmt.executeQuery("SELECT * FROM checkin where id = " + oldDotID +
					" and dtime > DATE_SUB(NOW(), INTERVAL 90 MINUTE);");
			
			if (rs.first()) {
				JSONArray allCheckins = new JSONArray();
				try {
					System.out.println(LocalDateTime.now() + ": " + "moved to first!");
					do {
						JSONObject singleLocation = new JSONObject();
						singleLocation.put("locationID", rs.getInt(1));
						singleLocation.put("checkinID", rs.getInt(2));
						singleLocation.put("lat", rs.getFloat(3));
						singleLocation.put("lng", rs.getFloat(4));
						singleLocation.put("dtime", rs.getDate(5));
						allCheckins.put(singleLocation);
					} while(rs.next());
				} catch (JSONException e) {
					System.out.println(LocalDateTime.now() + ": " + e.getMessage());
				}
				boolean movedDot = false;
				for(int i = 0; i< allCheckins.length(); i++) {
					JSONObject singleLocation = allCheckins.optJSONObject(i);
					Float selectingLat = (float) singleLocation.optDouble("lat");
					Float selectingLng = (float) singleLocation.optDouble("lng");
					 if ((selectingLat < latCeiling && selectingLat > latFloor) && (selectingLng < lngCeiling && selectingLng > lngFloor)) {
						 movedDot = true;
						 System.out.println(LocalDateTime.now() + ": " + "within time range and inside location bounds");
						 System.out.println(LocalDateTime.now() + ": " + "update checkin set lat = " + lat + ", lng = " + lng + ", dtime = NOW(), hash = '" 
						 + hash + "', colorCode = " + colorCode + " where id = " + oldDotID + ";");
						 stmt.execute("update checkin set lat = " + lat + ", lng = " + lng + ", dtime = NOW(), hash = '" 
						 + hash + "', colorCode = " + colorCode + " where id = " + oldDotID + ";");
						 return String.valueOf(oldDotID);
					 } else {
						 System.out.println(LocalDateTime.now() + ": " + "within time range, but outside of location bounds for location.");
					 }		
				}
				if (!movedDot) {
					 System.out.println(LocalDateTime.now() + ": " + "I found a dot within the time bound, but outside the range bound. Adding new, returning a new ID");
					 System.out.println(LocalDateTime.now() + ": " + "INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
								+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
					 PreparedStatement statement = conn.prepareStatement("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
								+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");",Statement.RETURN_GENERATED_KEYS);
					 int affectedRows = -1;
					 affectedRows = statement.executeUpdate();
					 System.out.println(LocalDateTime.now() + ": " + String.valueOf(affectedRows));
					 if (affectedRows <= 0) {
				            throw new SQLException("Creating location failed, no rows affected.");
				        }
					 try {
						 ResultSet generatedKeys = statement.getGeneratedKeys();
						 if (generatedKeys.next()) {
				            	Long rowID = generatedKeys.getLong(1);
				            	conn.close();
								return String.valueOf(rowID);
				            }
				            else {
				                throw new SQLException("Creating location failed, no ID obtained.");
				            }
				        } catch(SQLException ex) {
				        	System.out.println(LocalDateTime.now() + ": " + "Error: " + ex.getMessage());
				        }
				 }
			} else {
				//nothing returned from initial select
				System.out.println(LocalDateTime.now() + ": " + "didn't get anything within range... inserting location!");
				
				System.out.println(LocalDateTime.now() + ": " + "INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
						+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");");
			 PreparedStatement statement = conn.prepareStatement("INSERT INTO checkin (checkinid,lat,lng,dtime,hash,colorCode) values('101101','"
						+ lat + "','" + lng + "',NOW(),'" + hash + "'," + colorCode + ");",Statement.RETURN_GENERATED_KEYS);
			 int affectedRows = -1;
			 affectedRows = statement.executeUpdate();
			 System.out.println(LocalDateTime.now() + ": " + String.valueOf(affectedRows));
			 if (affectedRows <= 0) {
		            throw new SQLException("Creating location failed, no rows affected.");
		        }
			 try {
				 ResultSet generatedKeys = statement.getGeneratedKeys();
		            if (generatedKeys.next()) {
		            	Long rowID = generatedKeys.getLong(1);
		            	conn.close();
						return String.valueOf(rowID);
		            }
		            else {
		                throw new SQLException("Creating location failed, no ID obtained.");
		            }
		        } catch(SQLException ex) {
		        	System.out.println(LocalDateTime.now() + ": " + "Error: " + ex.getMessage());
		        }
			}
		} catch (SQLException ex) {
			System.out.println(LocalDateTime.now() + ": " + "Error: " + ex.getMessage());
		}
		System.out.println(LocalDateTime.now() + ": " + "Nothing returned from the initial select, likely a logic error if you ever see this in a log.");
		
		return String.valueOf(oldDotID);
	}
}