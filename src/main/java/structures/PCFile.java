package structures;

import process.Helper;

//This class is used to store all informations of the LAS-File and is used for calculations to determine the most relevant file.
public class PCFile {
	 public String location;
	 public int position; //position in List for fast access (coarse index)
	
	//Parameters of AABB Representation
	double[] min;
	double[] max;
	double[] center; 
	
	public PCFile(String location, double[] min, double[] max) {
		this.max=max;
		this.min=min;
		this.location=location;
		this.center= Helper.divide(Helper.add(max, min),2);
	}
	
	// Checks if this file intersects with the aabb of a view frustum
	// from: realtime collision detection (ISBN 9780080474144)
	public boolean aabbFrustumIntersection(double[] minFrustum, double[] maxFrustum) {
		if(this.max[0]< minFrustum[0]||this.min[0]>maxFrustum[0]) return false;
		if(this.max[1]< minFrustum[1]||this.min[1]>maxFrustum[1]) return false;
		if(this.max[2]< minFrustum[2]||this.min[2]>maxFrustum[2]) return false;
		return true;
	}
	
	// Checks is this file intersects with or is inside a plane
	// 1 inside (in normal direction)
	// -1 outside
	// 0 intersection
	// from: realtime collision detection (ISBN 9780080474144)
	public int aabbPlaneIntersection(Plane p) {
		double[] center = Helper.divide(Helper.add(max, min),2);
		double[] halfdiagonal = Helper.divide(Helper.subtract(max, min),2);
		// e: length of the projected halfdiagonal on the planes normal 
		double e = halfdiagonal[0]*Math.abs(p.normal[0])+halfdiagonal[1]*Math.abs(p.normal[1])+halfdiagonal[2]*Math.abs(p.normal[2]);
		double s = Helper.dot(center, p.normal)+p.d;// distance from plane to the box along the planes normal
		if(s>e)return 1;
		if(s+e<=0) return -1;
		return 0;
	}
	
	// returns distance if inside, otherwise -1 or 0 if intersecting
	public double minimalBoxToPlaneDistance(Plane p) {
		double[] halfdiagonal = Helper.divide(Helper.subtract(max, min),2);
		// e: length of the projected halfdiagonal on the planes normal 
		double e = halfdiagonal[0]*Math.abs(p.normal[0])+halfdiagonal[1]*Math.abs(p.normal[1])+halfdiagonal[2]*Math.abs(p.normal[2]);
		double s = Helper.dot(center, p.normal)+p.d;// distance from plane to the box along the planes normal
		if(s>e)return s-e;
		if(s+e<=0) return -1;
		return 0;
	}
	
	//true if point is in the positive half space (side in direction of planes normal) of the plane 
	public boolean pointToPlane(Plane plane, double[] point) {
		return Helper.dot(plane.normal, point)+plane.d>0;
	}
	
	
	//returns a value >= 0 if the Box is inside the frustum. This value is the minimal distance from the box to the near frustum plane. 
	public double detailedFrustumInformation(Frustum frustum) {
		for(Plane p: frustum.frustumPlanes) {
			int test = aabbPlaneIntersection(p);
			if(test==-1) return -1;//outside
			}
		return minimalBoxToPlaneDistance(frustum.nearPlane);// near frustum plane decides if box is inside or outside the frustum.
															// if inside the result is the distance to near frustum plane.
	}
	
	//Calculate the nearest point of the aabb box to the center of the near frustum plane.
	// from: realtime collision detection (ISBN 9780080474144)
	public double boxToNearCenterDistance(double[] nearCenter) {
		double[] nearestBoxPoint=new double[3];
		for(int i = 0; i<3;i++) {
			nearestBoxPoint[i]= Math.max(nearCenter[i], this.min[i]);
			nearestBoxPoint[i]= Math.min(nearestBoxPoint[i], this.max[i]);
		}
		// return the distance from the nearest point of the box to the center of the near frstum plane.
		return Helper.length(Helper.subtract(nearCenter, nearestBoxPoint));
	}
	
	//for debugging
	public String toString() {
		return location+": min:["+min[0]+","+min[1]+","+min[2]+"] max:["+max[0]+","+max[1]+","+max[2]+"]";
	}
	
	public double[] getmin() {
		return this.min;
	}
	
	public double[] getmax() {
		return this.max;
	}
}
