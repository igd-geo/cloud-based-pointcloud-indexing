package structures;

import java.io.Serializable;

import process.Main;

//This class represents a single Point
public class Point implements Serializable{
	long id;
	byte[] values;
	
	public Point(long id, byte[] values) {
		this.id=id;
		this.values=values;
	}
	public long getId() {
		return this.id;
	}
}
