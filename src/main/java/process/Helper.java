package process;

import java.util.ArrayList;

public class Helper {
	
	/**
	 * Normalizes a given number n
	 * @param n given number to normalize
	 * @param min smallest number as normalization basis
	 * @param precission of unnormalized numbers (E.G. 0.1 for numbers with one digit after comma, 0.01 for two digits,....)
	 * @return normalized value
	 */
	public static int normalize(double n, double min, double precission) {
		return (int) ((n-min)/precission);
	}
	
	// iterative approach to calculate the morton-code for the given coordinates. DEPREDCATED 
	public static long calculateIterativeMorton(int x, int y, int z) {
		long xx= (long) x;
		long yy= (long) y;
		long zz= (long) z;
		
		long result=0;
		int maxit=(int) Math.max(Math.max(Math.log(x)/Math.log(2), Math.log(y)/Math.log(2)),Math.log(z)/Math.log(2));
		
		for(int i=0;i<=maxit;i++) {
			result|= (((xx>>i)&1)<<3*i)|+(((yy>>i)&1)<<3*i+1)|+(((zz>>i)&1)<<3*i+2);
		}
		return result;
		
	}
	
	// magic numbers approach to calculate the morton-code for the given coordinates. Shifts each coordinate "truncate" bits right.
	public static long magicBitMorton(int x, int y, int z, int truncate) {
		long result = 0;
		long splitx;
		long splity;
		long splitz;
		long sign = 0;
		
		//For each coordinate adds two zero-bits between every bit. If negative, calculates the absolute value and sets the sign bit to 1.
		if(x<0) {
			sign=1;
			splitx=splitBy3(Math.abs(x),truncate);		
		}
		else {
			splitx=splitBy3(x,truncate);
		}
		if(y<0) {
			sign=(1<<1)|sign;
			splity=splitBy3(Math.abs(y),truncate);		
		}
		else {
			splity=splitBy3(y,truncate);
		}
		if(z<0) {
			sign=(1<<2)|sign;
			splitz=splitBy3(Math.abs(z),truncate);		
		}
		else {
			splitz=splitBy3(z,truncate);
		}
		
		//merge bits of all coordinates
		result |= splitx | splity<<1| splitz<<2;
		//add sign bits
		result |= (sign<<61);
		return result;
	}
	
	//Magic numbers approach. This function adds two 0-bits between every bit after shifting all bits "truncate" bits to the right.
	public static long splitBy3(int x, int truncate) {
		long result = x >> truncate;
		result = (result | result << 32) &  0x1F00000000FFFFL;
		result = (result | result << 16) &  0x1F0000FF0000FFL;
		result = (result | result << 8) & 0x100F00F00F00F00FL;
		result = (result | result << 4) & 0x10C30C30C30C30C3L;
		result = (result | result << 2) & 0x1249249249249249L;
		
		return result;
	}
	
	// This function removes the two 0-zero bits between every bit.
	public static int mergeFor3(long x) {
		long result = x & 0x1249249249249249L;
		result = (result|result>>2)& 0x10C30C30C30C30C3L;
		result=(result|result>>4)& 0x100F00F00F00F00FL;
		result=(result|result>>8)& 0x1F0000FF0000FFL;
		result=(result|result>>16)& 0x1F00000000FFFFL;
		result=(result|result>>32);
		return (int) result;
	}
	
	//This function calculates the Morton-Code range queries for a given AABB and returns them as one query as a string in cql.
	public static String calculateRangeQuery(int minX, int minY, int minZ, int maxX, int maxY, int maxZ) {
		ArrayList<Long[]> ranges = calculateRanges(minX, minY, minZ, maxX, maxY, maxZ);
		String result="partitionid>="+ranges.get(0)[0]+" and partitionid<="+ranges.get(0)[1];
		ranges.remove(0);
		for(Long[] r : ranges) {
			if(r[0]==r[1]) {
				result+=" and partitionid="+r[0];
			}
			else {
				result+=" and partitionid>="+r[0]+" and partitionid<="+r[1];
				}
			
		}
		return result;
		
	}
	
	//This function calculates the Morton-Code range queries for a given AABB and returns them as a ArrayList where each Array contains the
	//min value and max value for one query
	static ArrayList<Long[]> calculateRanges(int minX, int minY, int minZ, int maxX, int maxY, int maxZ){
		ArrayList<Long[]> result = new ArrayList<Long[]>();
		Long start = magicBitMorton(minX, minY, minZ, 0);
		Long end = magicBitMorton(maxX, maxY, maxZ, 0);
		result.add(new Long[]{start,start-1});
		//check which point between aabb_min and aabb_max is inside the aabb
		for(long mort = start; mort<=end;mort++) {
			//decode Mode-Code to get the coordinates
			int x=mergeFor3(mort);
			int y=mergeFor3(mort>>1);
			int z=mergeFor3(mort>>2);
			//check if point is inside aabb
			if(x>=minX&&x<=maxX&&y>=minY&&y<=maxY&&z>=minZ&&z<=maxZ) {
				//is last point is also inside aabb expand last query region.
				if(result.get(result.size()-1)[1]==mort-1) {
					result.get(result.size()-1)[1]=mort;
				}
				else {
					result.add(new Long[]{mort, mort});
				}
				
			}

		}
		return result;
	}
	
	
	
	//Splits 12 bits with magic numbers approach. The following schema results xxx 0 xxx 0 xxx 0 xxx.
	public static int split12(int x) {
		int result = x;
		result = ((result & 0x3F) | (result & 0x1FC0) << 2);
		result = ((result & 0x707) | (result & 0x3838) << 1);
		
		return result;
	}
	//Splits 4 bits with magic numbers approach. The following schema results x 0 0 0 x 0 0 0 x 0 0 0 x
	public static int split4(int x) {
		int result = x;
		result = (result | result << 6) & 0x303;
		result = (result | result << 3) & 0x1111;
		
		return result;
	}
	
	//Interleaves 12 bits with the bits (max. 4 bits) of the given level
	public static int extendByLOD(int last12FromMorton,int level) {
		int result = 0;
		result |= split12(last12FromMorton)<<1 | (short) split4(level);
		return result;
	}
	
	/**
	 * Calculates the final Node ID for all full patches in a spacing cell
	 * @param aktLod: level of the given patch
	 * @param totalLevels of the nested-octrees 
	 * @param merges: number of needed merges for the root spacing cells, to get the maximum number of points for the root nodes
	 * @param rootID: ID of the root node for the given patch
	 * @return final Node ID
	 */
	public static long calculateFinalIDForMerges(int aktLod, int totalLevels, int merges, long rootID) {
		long result = rootID>>((merges-aktLod)*3);
		result = result << ((totalLevels-1-aktLod)*3);
		int interleave = (int) (result & 4095);
		long extended = extendByLOD(interleave, aktLod);
		result = result >> 12;
		return (result<<15)|extended;
	}
	
	/**
	 * Calculates the final Node ID for all subpatches in a spacing cell
	 * @param aktLod: level of the given patch
	 * @param totalLevels of the nested-octrees 
	 * @param merges: number of needed merges for the root spacing cells, to get the maximum number of points for the root nodes
	 * @param rootID: ID of the root node for the given patch
	 * @param hashID: ID of the subregion in the given spacing cell
	 * @return final Node ID
	 */
	public static long calculateFinalIDForSplits(int aktLod, int aktSplitLevel, int totalLevels, long rootID, long hashID) {
		long result = rootID<<(aktSplitLevel*3);
		result= result|hashID;
		result =  result<<((totalLevels-1-aktLod)*3);
		int interleave = (int) (result & 4095);
		long extended = extendByLOD(interleave, aktLod);
		result = result >> 12;
		return (result<<15)|extended;
	}
	
	/**
	 * Calculates the final Node ID in the bottom-up approach
	 * @param aktLod: level of the given node
	 * @param totalLevels of the nested-octrees 
	 * @param aktID: id of the given node
	 * @return final Node ID
	 */
	public static long calculateFinalIDBoUp(int aktLod, int totalLevels, long aktID) {
		int interleave = (int) (aktID & 4095);
		long extended = extendByLOD(interleave, aktLod);
		long result = aktID >> 12;
		return (result<<15)|extended;
		
	}
	
	//transforms a three coordinates to a byte-array
	public static byte[] pointToByte(int x,int y, int z) {
		return new byte[] {
	            (byte)(x >> 24),
	            (byte)(x >> 16),
	            (byte)(x >> 8),
	            (byte)x,
	            (byte)(y >> 24),
	            (byte)(y >> 16),
	            (byte)(y >> 8),
	            (byte)y,
	            (byte)(z >> 24),
	            (byte)(z >> 16),
	            (byte)(z >> 8),
	            (byte)z};
	}
	
	//transforms a byte-array to an int value
	public static int byteToInt(byte[] b) {
	    return ((b[0] & 0xFF) << 24) | 
	            ((b[1] & 0xFF) << 16) | 
	            ((b[2] & 0xFF) << 8 ) | 
	            ((b[3] & 0xFF) << 0 );
	}
	
	//calculates the cross product of two arrays with three entries (two 3d vectors)
	public static double[] crossProduct(double[] a, double[] b) {
		return new double[] {a[1]*b[2]-b[1]*a[2],a[2]*b[0]-b[2]*a[0],a[0]*b[1]-b[0]*a[1]};
	}
	
	
	//adds two arrays a+b (two nd vectors)
	public static double[] add(double[]a,double[]b) {
		double[] result = new double[a.length];
		for(int i=0;i<a.length;i++) {
			result[i]=a[i]+b[i];
		}
		return result;
	}
	
	//subtracts two arrays a-b (two nd vectors)
	public static double[] subtract(double[]a,double[]b) {
		double[] result = new double[a.length];
		for(int i=0;i<a.length;i++) {
			result[i]=a[i]-b[i];
		}
		return result;
	}
	
	//calculates the dot product of two arrays (two nd vectors)
	public static double dot(double[]a,double[]b) {
		double result=0;
		for(int i=0;i<a.length;i++) {
			result+=a[i]*b[i];
		}
		return result;
	}
	
	//divides all entries of the given array a with the value z
	public static double[] divide(double[]a,double z) {
		double[] result = new double[a.length];
		for(int i=0;i<a.length;i++) {
			result[i]=a[i]/z;
		}
		return result;
	}
	
	//length of the vector ||a||
	public static double length(double[]a) {
		double sq=0;
		for(int i=0;i<a.length;i++) {
			sq+=Math.pow(a[i],2);
		}
		return Math.sqrt(sq);
	}
	
	//distance between x and y
	public static double distance(int x1,int y1,int z1, int x2, int y2, int z2) {
		double sq = (int) Math.pow(x1-x2, 2)+Math.pow(y1-y2, 2)+Math.pow(z1-z2, 2);
		
		return Math.sqrt(sq);
	}


}
