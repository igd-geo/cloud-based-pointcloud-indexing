package process;

import org.apache.spark.sql.Row;

public class Parser {
	
	//This function parses a read Row from the las file via IQmulus to a byte array. The different implemented cases are based on the source code
	//from the IQmulus library and differ in the length of the row.
	public static byte[] getRowAsBytes(Row r,int x, int y, int z){
		//All cases include the x,y,z coordinates and intensity. So transform them first into a byte array
		byte[] base = convertGroup1ToBytes(x, y, z, r.getShort(5));
		
		//Transform all other given properties of a point
		if(r.length()==11) {
			byte[] result = new byte[20];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==12) {
			byte[] result = new byte[28];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			byte[] time = convertDoubleToBytes(r.getDouble(11));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==14) {
			byte[] result = new byte[26];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			byte[] rgb = convertRGBToBytes(r.getShort(11),r.getShort(12),r.getShort(13));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==15) {
			byte[] result = new byte[34];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			byte[] time = convertDoubleToBytes(r.getDouble(11));
			byte[] rgb = convertRGBToBytes(r.getShort(12),r.getShort(13),r.getShort(14));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==13) {
			byte[] result = new byte[30];
			byte[] group2 = convertGroup3ToBytes(r.getByte(7), r.getByte(8), r.getShort(10), r.getByte(9), r.getShort(11));
			byte[] time = convertDoubleToBytes(r.getDouble(12));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			result[i]=r.getByte(6); 
			i++;
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			
		}
		
		else if(r.length()==16) {
			byte[] result = new byte[36];
			byte[] group2 = convertGroup3ToBytes(r.getByte(7), r.getByte(8), r.getShort(10), r.getByte(9), r.getShort(11));
			byte[] time = convertDoubleToBytes(r.getDouble(12));
			byte[] rgb = convertRGBToBytes(r.getShort(13),r.getShort(14),r.getShort(15));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			result[i]=r.getByte(6); 
			i++;
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
		}
		else if(r.length()==17) {
			byte[] result = new byte[38];
			byte[] group2 = convertGroup3ToBytes(r.getByte(7), r.getByte(8), r.getShort(10), r.getByte(9), r.getShort(11));
			byte[] time = convertDoubleToBytes(r.getDouble(12));
			byte[] rgb = convertRGBToBytes(r.getShort(13),r.getShort(14),r.getShort(15));
			byte[] nir = convertShortToBytes(r.getShort(16));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			result[i]=r.getByte(6); //returns
			i++;
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
			for(byte b: nir) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==19) {
			byte[] result = new byte[57];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			byte[] time = convertDoubleToBytes(r.getDouble(11));
			byte[] wp = convertWavePacketsToBytes(r.getByte(12),r.getLong(13),r.getInt(14),r.getFloat(15),r.getFloat(16),r.getFloat(17),r.getFloat(18));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: wp) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==22) {
			byte[] result = new byte[63];
			byte[] group2 = convertGroup2ToBytes(r.getByte(6), r.getByte(7), r.getByte(8), r.getByte(9), r.getShort(10));
			byte[] time = convertDoubleToBytes(r.getDouble(11));
			byte[] rgb = convertRGBToBytes(r.getShort(12),r.getShort(13),r.getShort(14));
			byte[] wp = convertWavePacketsToBytes(r.getByte(15),r.getLong(16),r.getInt(17),r.getFloat(18),r.getFloat(19),r.getFloat(20),r.getFloat(21));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
			for(byte b: wp) {
				result[i]=b;
				i++;
			}
			return result;
		}
		else if(r.length()==58) {
			byte[] result = new byte[58];
			byte[] group2 = convertGroup3ToBytes(r.getByte(7), r.getByte(8), r.getShort(10), r.getByte(9), r.getShort(11));
			byte[] time = convertDoubleToBytes(r.getDouble(12));
			byte[] wp = convertWavePacketsToBytes(r.getByte(13),r.getLong(14),r.getInt(15),r.getFloat(16),r.getFloat(17),r.getFloat(18),r.getFloat(19));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			result[i]=r.getByte(6); 
			i++;
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: wp) {
				result[i]=b;
				i++;
			}
			return result;
		}
			//return 9;
		else if(r.length()==24) {
			byte[] result = new byte[67];
			byte[] group2 = convertGroup3ToBytes(r.getByte(7), r.getByte(8), r.getShort(10), r.getByte(9), r.getShort(11));
			byte[] time = convertDoubleToBytes(r.getDouble(12));
			byte[] rgb = convertRGBToBytes(r.getShort(13),r.getShort(14),r.getShort(15));
			byte[] nir = convertShortToBytes(r.getShort(16));
			byte[] wp = convertWavePacketsToBytes(r.getByte(17),r.getLong(18),r.getInt(19),r.getFloat(20),r.getFloat(21),r.getFloat(22),r.getFloat(23));
			int i = 0;
			for(byte b: base) {
				result[i]=b;
				i++;
			}
			result[i]=r.getByte(6); 
			i++;
			for(byte b: group2) {
				result[i]=b;
				i++;
			}
			for(byte b: time) {
				result[i]=b;
				i++;
			}
			for(byte b: rgb) {
				result[i]=b;
				i++;
			}
			for(byte b: nir) {
				result[i]=b;
				i++;
			}
			for(byte b: wp) {
				result[i]=b;
				i++;
			}
			return result;
		}
		return null;
	}
	
	//converts x,y,z,intensity
	public static byte[] convertGroup1ToBytes(int x, int y, int z, short intensity) {
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
	            (byte)z,
	            (byte)(intensity >> 8),
	            (byte)(intensity)
	            };
		
	}
	//converts flags, classification,angle,user, source
	public static byte[] convertGroup2ToBytes(byte flags,byte classification,byte angle,byte user,short source) {
		return new byte[] {
				flags,
				classification,
				angle,
				user,
				(byte)(source >> 8),
				(byte)(source)
		};
	}
	
	//converts flags, classification,angle (short),user, source
	public static byte[] convertGroup3ToBytes(byte flags,byte classification,short angle,byte user,short source) {
		return new byte[] {
				flags,
				classification,
				(byte)(angle>>8),
				(byte)(angle),
				user,
				(byte)(source >> 8),
				(byte)(source)
		};
	}
	
	//converts gps-time
	public static byte[] convertDoubleToBytes(double d) {
		long x = Double.doubleToLongBits(d);
		return new byte[] {
	            (byte)(x >> 56),
				(byte)(x >> 48),
	            (byte)(x >> 40),
	            (byte)(x >> 32),
	            (byte)(x >> 24),
	            (byte)(x >> 16),
	            (byte)(x >> 8),
	            (byte)x
	            };
	}
	//converts nir
	public static byte[] convertShortToBytes(short s) {
		return new byte[] {
				(byte)(s >> 8),
				(byte)(s)
		};
	}
	
	//converts color:rgb
	public static byte[] convertRGBToBytes(short r, short g, short b) {
		return new byte[] {
				(byte)(r >> 8),
				(byte)(r),
				(byte)(g >> 8),
				(byte)(g),
				(byte)(b >> 8),
				(byte)(b)
		};
	}
	
	//converts Wave Packets
	public static byte[] convertWavePacketsToBytes(byte index,long offset,int size,float location, float xt, float yt, float zt) {
		int iloc = Float.floatToIntBits(location);
		int ixt = Float.floatToIntBits(xt);
		int iyt = Float.floatToIntBits(yt);
		int izt = Float.floatToIntBits(zt);
		return new byte[] {
				index,
				(byte)(offset >> 56),
				(byte)(offset >> 48),
	            (byte)(offset >> 40),
	            (byte)(offset >> 32),
	            (byte)(offset >> 24),
	            (byte)(offset >> 16),
	            (byte)(offset >> 8),
	            (byte)offset,
	            (byte)(size >> 24),
	            (byte)(size >> 16),
	            (byte)(size >> 8),
	            (byte)size,
	            (byte)(iloc >> 24),
	            (byte)(iloc >> 16),
	            (byte)(iloc >> 8),
	            (byte)iloc,
	            (byte)(ixt >> 24),
	            (byte)(ixt >> 16),
	            (byte)(ixt >> 8),
	            (byte)ixt,
	            (byte)(iyt >> 24),
	            (byte)(iyt >> 16),
	            (byte)(iyt >> 8),
	            (byte)iyt,
	            (byte)(izt >> 24),
	            (byte)(izt >> 16),
	            (byte)(izt >> 8),
	            (byte)izt
	            
				
		};
	}
	
	

}
