package structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

//This class is used to store the intermediate results for the bottom-up approach. I.E. a node of the octree.
public class IntermedPatch implements Serializable{
	public long cellid; // Node ID
	public long partitionid; // ids of roots
	public ArrayList<Point> points;
	public int level;
	public boolean result=false;// True if this node is finished and can be stored in database.
	
	public IntermedPatch(long cellid, long partitionID,ArrayList<Point> poi,int level) {
		this.cellid=cellid;
		this.partitionid = partitionID;
		this.level=level;
		this.points=poi;
		}
	}
	