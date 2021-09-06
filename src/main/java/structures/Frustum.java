package structures;

//This class represents a view frustum
public class Frustum {
	double[][] nearPlanePoints; //Points must be sorted clockwise
	double[][] farPlanePoints; //Points must be sorted clockwise and must correspond to the order of nearPlane.
	public Plane[] frustumPlanes; // Array of all planes from the frustum without the near Plane.
									//Plane normals are pointing inside the frustum.
	public NearFrustumPlane nearPlane; // Near Frustum Plane
	
	public double[] min,max; //AABB informations. Smallest and biggest point.
	
	public Frustum(double[][] nearPlanePoints, double[][] farPlanePoints) {
		this.nearPlanePoints=nearPlanePoints;
		this.farPlanePoints=farPlanePoints;
		calculateFrustumPlanes();
		
	}
	
	//This function calculates the planes of the frustum from the given points
	public void calculateFrustumPlanes() {
		this.frustumPlanes= new Plane[5];
		for(int i=0;i<3;i++) {
			this.frustumPlanes[i]= new Plane(farPlanePoints[i],nearPlanePoints[i],nearPlanePoints[i+1]);
		}
		this.frustumPlanes[3]= new Plane(farPlanePoints[3],nearPlanePoints[3],nearPlanePoints[0]);
		this.frustumPlanes[4]= new Plane(farPlanePoints[0],farPlanePoints[1],farPlanePoints[2]);
		
		this.nearPlane= new NearFrustumPlane(nearPlanePoints[2],nearPlanePoints[1],nearPlanePoints[0]);
	}
	


}
