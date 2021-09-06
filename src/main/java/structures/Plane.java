package structures;

import process.Helper;

//This class represents a Plane
public class Plane {

	public double [] normal;//points inside the view frustum!
	double d;
	// a----b----c
	public Plane(double[] a, double[] b, double[] c) {
		this.normal= Helper.crossProduct(Helper.subtract(a, b), Helper.subtract(c, b));
		this.normal=Helper.divide(this.normal, Helper.length(this.normal));
		this.d=-(Helper.dot(this.normal,b));
	}
	public Plane() {
	}
	
}
