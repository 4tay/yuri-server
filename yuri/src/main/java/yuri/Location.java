package yuri;

import java.time.LocalDateTime;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import com.sun.jersey.spi.resource.Singleton;


@Singleton
@Path("/location")
public class Location {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getLocal(@QueryParam("range") Float range, @QueryParam("lat") Float lat,
			@QueryParam("lng") Float lng) {
		DBConnection conn = new DBConnection();
		
		if(range == null) {
			range = (float) 0.0;	
		}
		if(lat == null) {
			lat = (float) 0.0;
		}
		if(lng == null) {
			lng = (float) 0.0;
		}
		
		System.out.println(LocalDateTime.now() + ": " + "Requesting location! " + lat + ", " + lng + ", " + range);
		return conn.getEventsInRange(range, lng, lat);
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public String addLocal(@QueryParam("id") int id, @QueryParam("lat") Float lat,
			@QueryParam("lng") Float lng, @QueryParam("colorCode") int colorCode, @QueryParam("checkin") int checkin,
			@QueryParam("hash") String hash) {
		
		if(hash == null || hash.equals("")) {
			hash = "#emptyHash";
		} else if( hash.charAt(0) != '#') {
			hash = "#" + hash;	
		}
		if(colorCode <=0) {
			colorCode = 0;
		}
		if(lat == null) {
			lat = (float) 0.0;
		}
		if(lng == null) {
			lng = (float) 0.0;
		}
		if(checkin <= 0) {
			checkin = 101101;
		}
		
		DBConnection conn = new DBConnection();
		
		System.out.println(LocalDateTime.now() + ": " + "Print location info... " + checkin + ", " + lat + ", " + lng + ", " + hash + ", " + colorCode );
		
		return conn.addLocation(checkin, lat, lng, hash, colorCode);
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public String updateNewLocation(@QueryParam("oldDotID") int oldDotID, @QueryParam("potentialDotID") int potentialDotID,
			@QueryParam("lat") Float lat, @QueryParam("lng") Float lng, @QueryParam("hash") String hash,
			@QueryParam("colorCode") int colorCode) {
		if(oldDotID <= 0) {
			oldDotID = 0;
		}
		if(potentialDotID <= 0) {
			potentialDotID = 0;
		}
		if(hash == null || hash.equals("")) {
			hash = "#emptyHash";
		}
		if(colorCode <=0) {
			colorCode = 0;
		}
		if(lat == null) {
			lat = (float) 0.0;
		}
		if(lng == null) {
			lng = (float) 0.0;
		}

		System.out.println(LocalDateTime.now() + ": " + "Print location info... " + oldDotID + ", " + potentialDotID + ", " + lat + ", " + lng + ", " + hash + ", " + colorCode );

		DBConnection conn = new DBConnection();	
		return conn.putLocation(oldDotID, lat, lng, hash, colorCode);
	}
	@Path("/search")
	@GET
	public String getTags(@QueryParam("hash") String hash) {
		return hash;
	}
	
}