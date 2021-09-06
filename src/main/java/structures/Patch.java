package structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import process.Helper;

//This class represents a calculated Patch of points.
public class Patch implements Serializable {
	long cellid; // Node iD
	long partitionid; // ids of roots
	List<byte[]> points;
	
	public Patch(long cellid, long partitionID, Point firstPoint) {
		this.cellid=cellid;
		this.partitionid = partitionID;
		this.points=new ArrayList<byte[]>();
		addPoint(firstPoint);
	}
	
	public Patch(long cellid, long partitionID,Collection<? extends Point> poi) {
		this.cellid=cellid;
		this.partitionid = partitionID;
		this.points=new ArrayList<byte[]>();
		for(Point p:poi) {
			this.addPoint(p);
		}
	}

	
	public void addPoint(Point point) {
		this.points.add(point.values);
	}
	public void addAllPoints(List<byte[]> l) {
		this.points.addAll(l);
	}
	public long getcellid() {
		return this.cellid;
	}

	public long getpartitionid() {
		return this.partitionid;
	}
	
	
	public List<byte[]> getpoints(){

		return this.points;
	}
	
	//For debugging	
	public String stringOfPoints() {
		String result="";
        for(byte[] b : points) {
        	int x = Helper.byteToInt(Arrays.copyOfRange(b, 0, 4));
        	int y = Helper.byteToInt(Arrays.copyOfRange(b, 4, 8));
        	int z = Helper.byteToInt(Arrays.copyOfRange(b, 8, 12));
        	result= result+" |X:"+x+" Y: "+y+" Z: "+z;
        }
        return result;
	}
	
	//For debugging
	/*
	public String toString() {
		return "partitionID: "+this.partitionid+" Cellid: "+Long.toBinaryString(this.cellid)+" NumOfPoints: "+this.points.size();
	}
	*/

}
