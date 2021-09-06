package structures;

//This class represents extended Informations of a single Point
public class PointExtended extends Point {
	public int xCellCenter, yCellCenter,zCellCenter;
	public int x,y,z;
	public double distance;

	public PointExtended(long id, byte[] values, int x, int y, int z) {
		super(id, values);
		this.x= x;
		this.y= y;
		this.z= z;
	}

}
