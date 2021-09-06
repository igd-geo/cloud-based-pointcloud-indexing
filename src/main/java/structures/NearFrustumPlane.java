package structures;

import process.Helper;

//This class represents the Near Clipping Plane of the frustum.
public class NearFrustumPlane extends Plane {
	public double[] center;

	public NearFrustumPlane(double[] a, double[] b, double[] c) {
		super();
		double[] vector1 = Helper.subtract(a, b);
		double[] vector2 = Helper.subtract(c, b);
		this.normal= Helper.crossProduct(vector1, vector2);
		this.normal=Helper.divide(this.normal, Helper.length(this.normal));
		this.d=-(Helper.dot(this.normal,b));
		//b+1/2*Vector1+1/2*Vector2
		this.center = Helper.add(b, Helper.add(Helper.divide(vector1, 2), Helper.divide(vector2, 2)));
	}

}
