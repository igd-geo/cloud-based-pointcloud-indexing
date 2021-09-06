package structures;

import java.util.ArrayList;

//This class is used to store all relevant informations of a las-file
public class FileInformations {
	
	double minX;
	double minY;
	double minZ;
	double maxX;
	double maxY;
	double maxZ;
	double scale;
	
	public FileInformations(double minX, double minY, double minZ, double maxX, double maxY, double maxZ, double scale) {
		this.minX=minX;
		this.minY=minY;
		this.minZ=minZ;
		this.maxX=maxX;
		this.maxY=maxY;
		this.maxZ=maxZ;
		this.scale=scale;
	}
	
	public double getMinX() {
		return this.minX;
	}
	
	public double getMinY() {
		return this.minY;
	}
	
	public double getMinZ() {
		return this.minZ;
	}
	public double getMaxX() {
		return this.maxX;
	}
	
	public double getMaxY() {
		return this.maxY;
	}
	
	public double getMaxZ() {
		return this.maxZ;
	}
	
	public double getScale() {
		return this.scale;
	}

}
