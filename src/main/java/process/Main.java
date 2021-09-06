package process;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.github.mreutegg.laszip4j.LASHeader;
import com.github.mreutegg.laszip4j.LASReader;

import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;
import structures.ExtendedFileInformations;
import structures.FileInformations;
import structures.Frustum;
import structures.IntermedPatch;
import structures.PCFile;
import structures.Patch;
import structures.Point;
import structures.PointExtended;


public class Main {
	static String folderLocation; //location of the file with las-files
	
	// This function represents the initialization for the progressive indexing. It extracts all important informations from the las-files
	// and returns them as ExtendedFileInformations-Object
	public static ExtendedFileInformations indexFiles(String location){
		File folder = new File(location);
		String[] listFiles = folder.list();
		ArrayList<PCFile> dataIndex = new ArrayList<PCFile>();
		double minX=Double.MAX_VALUE;
		double minY=Double.MAX_VALUE;
		double minZ=Double.MAX_VALUE;
		double scale=Double.MAX_VALUE;
		
		//iterates thought all files and extracts the important informations
		for(String s: listFiles) {
			LASReader reader = new LASReader(new File(location+s));
			LASHeader header = reader.getHeader();
			//creates a PCFile-Object with the Informations of the BoundingBox and the file path
			dataIndex.add(new PCFile(location+s,new double[] {header.getMinX(),header.getMinY(),header.getMinZ()},
					new double[] {header.getMaxX(),header.getMaxY(),header.getMaxZ()}));
			if(header.getMinX()<minX) minX=header.getMinX();
			if(header.getMinY()<minY) minY=header.getMinY();
			if(header.getMinZ()<minZ) minZ=header.getMinZ();
			if(header.getXScaleFactor()<scale) scale=header.getXScaleFactor();
			if(header.getYScaleFactor()<scale) scale=header.getYScaleFactor();
			if(header.getZScaleFactor()<scale) scale=header.getZScaleFactor();
		}
		return new ExtendedFileInformations(dataIndex, minX, minY, minZ, scale);
	}
	
	// This function represents the initialization for the standard indexing (not progressive). It extracts all important 
	// informations from the las-files and returns them as FileInformations-Object
	public static FileInformations getFileInfos(String location) {
		File folder = new File(location);
		String[] listFiles = folder.list();
		double minX=Double.MAX_VALUE;
		double minY=Double.MAX_VALUE;
		double minZ=Double.MAX_VALUE;
		double maxX = Double.MIN_VALUE;
		double maxY = Double.MIN_VALUE;
		double maxZ = Double.MIN_VALUE;
		double scale=Double.MAX_VALUE;
		
		//iterates thought all files and extracts the important informations
		for(String s: listFiles) {
			LASHeader header;
			LASReader reader = new LASReader(new File(location+s));
			header = reader.getHeader();

			if(header.getMinX()<minX) minX=header.getMinX();
			if(header.getMinY()<minY) minY=header.getMinY();
			if(header.getMinZ()<minZ) minZ=header.getMinZ();
			if(header.getMaxX()>maxX) maxX=header.getMaxX();
			if(header.getMaxY()>maxY) maxY=header.getMaxY();
			if(header.getMaxZ()>maxZ) maxZ=header.getMaxZ();
			if(header.getXScaleFactor()<scale) scale=header.getXScaleFactor();
			if(header.getYScaleFactor()<scale) scale=header.getYScaleFactor();
			if(header.getZScaleFactor()<scale) scale=header.getZScaleFactor();
			}
		return new FileInformations(minX, minY, minZ,maxX,maxY,maxZ, scale);
		
	}
	
	//chooses the next to be indexed file (=most relevant), given all remainingFiles and the actual users ViewFrustum
	public static PCFile getMostRelevantFile(ArrayList<PCFile> remainingFiles, Frustum frustum) {
		
		// AABB-Informations of the given View Frustum
		double [] min = frustum.min;
		double [] max = frustum.max;
		
		PCFile result=null; //stores the actual best PCFile
		double bestDistance = Double.MAX_VALUE; //stores the distance of the stored PCFile to the NCP
		int position=-1; // stores the position (pointer) to the stores PCFile in the remainingFiles list.
		
		//iterates through all remaining files
		int i = -1;
		for(PCFile file : remainingFiles) {
			i++;
			// determine if file is inside aabb of view frustum
			if(file.aabbFrustumIntersection(min, max)) {
				double info = file.detailedFrustumInformation(frustum);
				if(info==-1) continue; // outside frustum
				if(info<bestDistance) {
					bestDistance=info;
					result=file;
					position=i;
				}
			}
		}
		
		//if there is a file inside the view frustum, return the nearest.
		if(result!=null) {
			result.position=position;
			return result;
		}
		
		//go on and determine the nearest file (center) outside the view frustum to the center of NCP
		i=-1;
		double [] nearCenter = frustum.nearPlane.center;
		for(PCFile file : remainingFiles) {
			i++;
			double distance = file.boxToNearCenterDistance(nearCenter);
			if(distance<bestDistance) {
				bestDistance=distance;
				result=file;
				position=i;
			}
		}
		result.position=position;
		return result;
		}
	
	
	
	//main-method, executed from Spark
	public static void main(String[] args) {
		
		SparkConf conf;
		
		// Configurations for the executed application.
		
		// Uncomment this part for local debugging
		//=============================================================================
		/*
		conf = new SparkConf().setAppName("Test").setMaster("local[*]");
		conf.set("spark.cassandra.connection.host", "127.0.0.1");
		
		//conf.set("spark.hadoop.mapred.max.split.size", "10000000");
		conf.set("spark.cassandra.output.batch.grouping.key","none");
		conf.set("spark.cassandra.output.concurrent.writes","32");
		folderLocation="/home/kekocon/Documents/Punktwolken/r/";
		final int spacing=8; // length of a spacing cell is: (2^spacing)*scale 
		final int levels = 7; 
		if(levels>spacing)System.err.println("The number of levels must not be greater than spacing factor!");
		final int maxPointsPerCell = 32769;
		final int mode=2; // sampling strategy: 0= middle point of a virtual cell; 1= first point of a virtual cell; 2= random points; 
		// 3,4= different versions for random sampling, deprecated due to bad visual results
		final int dir = 0; // change to 1 to evaluate the bottom-up approach
		final boolean progressive = false;
		final boolean newtable = true;
		final String keyspace = "ptab";
		final String table = "test";
		final String replication = "1";
		final boolean addToRegion= false;
		
		final int maxPointPerDim = (int) Math.pow(maxPointsPerCell+1, ((double)1/3));
		final int mergingFactor = (int) Math.floor(Math.log(maxPointPerDim)/Math.log(2));
		*/
		//======================================================================
		
		// Uncomment this part for execution via command line in a spark cluster
		//======================================================================
		
		folderLocation=args[10];
		final int spacing= Integer.parseInt(args[7]); 
		final int levels = Integer.parseInt(args[9]); 
		final int maxPointsPerCell = Integer.parseInt(args[6]);
		final int mode= Integer.parseInt(args[8]);
		final int dir = 0; //Integer.parseInt(args[8]); Change this to "1" to evaluate the bottom-up approach
		
		if(levels>spacing)System.err.println("The number of levels must not be greater than spacing factor!");
		final boolean progressive = Boolean.parseBoolean(args[12]);
		final boolean newtable = Boolean.parseBoolean(args[2]);
		final String keyspace = args[3];
		final String table = args[4];
		final String replication = args[5];
		final boolean addToRegion= Boolean.parseBoolean(args[11]);
		final int maxPointPerDim = (int) Math.pow(maxPointsPerCell+1, ((double)1/3));
		final int mergingFactor = (int) Math.floor(Math.log(maxPointPerDim)/Math.log(2));
		double minOldx = 0, minOldy= 0,minOldz=0;
		int truncateOld=0;
		
		
		conf = new SparkConf().setAppName("Pointcloud Indexing").setMaster(args[0]);
		conf.set("spark.cassandra.connection.host", args[1]);
		//conf.set("spark.cassandra.output.batch.grouping.key","none");
		conf.set("spark.cassandra.output.concurrent.writes","1000");
		if(Integer.parseInt(args[13])!=-1) {conf.set("spark.hadoop.mapred.max.split.size", args[13]);}
		if(addToRegion) {
			minOldx=Double.parseDouble(args[14]);
			minOldy=Double.parseDouble(args[15]);
			minOldz=Double.parseDouble(args[16]);
			truncateOld=Integer.parseInt(args[17]);
		}
		
		//================================================
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlc = new SQLContext(sc);
		
		//initialize new Cassandra-Table with given values
		if(newtable) {
			CassandraConnector connector = CassandraConnector.apply(sc.getConf());
			try (Session session = connector.openSession()) {
				session.execute("CREATE KEYSPACE "+keyspace+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "+replication+"}");
	            session.execute("CREATE TABLE "+keyspace+"."+table+" (partitionid BIGINT , cellid BIGINT, points frozen<set<BLOB>>"
	            		+ ",PRIMARY KEY (partitionid, cellid))");
			}
		}
		
		//Initialize Application....
		double minX,minY,minZ,scale;
		double maxX=0;
		double maxY=0;
		double maxZ=0;
		ArrayList remainingFiles;
		
		//... for progressive Indexing
		if(progressive) {
			ExtendedFileInformations fileInfos = indexFiles(folderLocation);
			minX=fileInfos.getMinX();
			minY=fileInfos.getMinY();
			minZ=fileInfos.getMinZ();
			scale=fileInfos.getScale();
			remainingFiles=fileInfos.getRemainingFiles();
			}
		
		//... for standard Indexing
		else {
			FileInformations fileInfos = getFileInfos(folderLocation);
			minX=fileInfos.getMinX();
			minY=fileInfos.getMinY();
			minZ=fileInfos.getMinZ();
			maxX=fileInfos.getMaxX();
			maxY=fileInfos.getMaxY();
			maxZ=fileInfos.getMaxZ();
			scale=fileInfos.getScale();
			File folder = new File(folderLocation);
			remainingFiles = new ArrayList(Arrays.asList(folder.list()));
			
			
		}
		
		//Index each file....
		while(!remainingFiles.isEmpty()) {
			String fileLocation;
			PCFile nextFile=null; //for progressive approach
			//...progressive 
			if(progressive) {
				Frustum f=null; // Query the actual view frustum at this place
				nextFile = getMostRelevantFile((ArrayList<PCFile>) remainingFiles, f);
				fileLocation=nextFile.location;
				remainingFiles.remove(nextFile.position);
			}
			//...as one bulk
			else {
				fileLocation=folderLocation+"*.las";
				remainingFiles= new ArrayList();
			}

			double xScale=scale;
			double yScale =scale;
			double zScale=scale;
			double xOff=0;
			double yOff= 0;
			double zOff=0;
			int normMaxX=Helper.normalize(maxX, minX, scale);
			int normMaxY=Helper.normalize(maxY, minY, scale);
			int normMaxZ=Helper.normalize(maxZ, minZ, scale);
			int normedMaxValue = Math.max(normMaxX, Math.max(normMaxY, normMaxZ));
			int truncate = (addToRegion) ? truncateOld : Math.max(13-Integer.numberOfLeadingZeros(normedMaxValue),0);// shift each coordinate with this value, 
																					  // to get a maximum 19 bit value
																					 // 19*3 + 4 (level bits) + 3 (sign bits)=64.
			
			if(addToRegion) {
				normMaxX=Helper.normalize(maxX, minOldx, scale);
				normMaxY=Helper.normalize(maxY, minOldy, scale);
				normMaxZ=Helper.normalize(maxZ, minOldz, scale);
			}
			
			final int shift=(spacing-truncate)*3; //shift value for grouping points in spacing cells
			final long shiftedValueMask = (long) Math.pow(2, shift)-1; //mask used in further LoD calculations
			
	
			
			//===============================================================================================//
			// This next part defines all helper functions used in the Spark point cloud processing pipeline //
			//===============================================================================================//
			
			//This (Map)-function calculates the Morton-Code and groupID for each point 
			final class calcIndex extends AbstractFunction1<Row, Tuple2<Long,Point>> implements Serializable{
	
				@Override
				public Tuple2<Long,Point> apply(Row row) {
					double x= row.getInt(2)*xScale+xOff;
					double y= row.getInt(3)*yScale+yOff;
					double z= row.getInt(4)*zScale+zOff;
					
					
					int normX=Helper.normalize(x, minX, scale);
					int normY=Helper.normalize(y, minY, scale);
					int normZ=Helper.normalize(z, minZ, scale);
					long id= Helper.magicBitMorton(normX, normY, normZ,truncate);
					byte[] values = Parser.getRowAsBytes(row, normX, normY, normZ);
					return new Tuple2<Long,Point>(id>>shift,new Point(id,values));
					}
				}
			
			//This (Map)-function calculates the Morton-Code and groupID for each point. This function is used in case of cell center sampling.
			final class calcIndexExtended extends AbstractFunction1<Row, Tuple2<Long,PointExtended>> implements Serializable{
				
				@Override
				public Tuple2<Long,PointExtended> apply(Row row) {
					
					double x= row.getInt(2)*xScale+xOff;
					double y= row.getInt(3)*yScale+yOff;
					double z= row.getInt(4)*zScale+zOff;
					
					int normX=Helper.normalize(x, minX, scale);
					int normY=Helper.normalize(y, minY, scale);
					int normZ=Helper.normalize(z, minZ, scale);
					long id= Helper.magicBitMorton(normX, normY, normZ, truncate);
					byte[] values = Parser.getRowAsBytes(row, normX, normY, normZ);
					return new Tuple2<Long,PointExtended>(id>>shift,new PointExtended(id,values,normX,normY,normZ));
					}
				}
			
			//This (Map)-function calculates the Morton-Code and groupID for each point. This function is used if the bottom-up approach was chosen
			final class calcIndexBoUp extends AbstractFunction1<Row, Tuple2<Long,Point>> implements Serializable{
				
				@Override
				public Tuple2<Long,Point> apply(Row row) {
					
					double x= row.getInt(2)*xScale+xOff;
					double y= row.getInt(3)*yScale+yOff;
					double z= row.getInt(4)*zScale+zOff;
					
					
					int normX=Helper.normalize(x, minX, xScale);
					int normY=Helper.normalize(y, minY, yScale);
					int normZ=Helper.normalize(z, minZ, zScale);
					long id= Helper.magicBitMorton(normX, normY, normZ,truncate);
					byte[] values = Parser.getRowAsBytes(row, normX, normY, normZ);
					return new Tuple2<Long,Point>(id>>((spacing+mergingFactor-levels+1-truncate)*3),new Point(id,values));
					}
				}
			
			// This function calculates the level of detail for all points in the given spacing-cell.
			// In this function the first point from each virtual cell will be chosen
			PairFlatMapFunction <Tuple2<Long,Iterable<Point>>,Long,Patch> calcLODFirst = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<Point>>,Long,Patch>(){

						@Override
						public Iterable<Tuple2<Long, Patch>> call(Tuple2<Long, Iterable<Point>> t) throws Exception 
						{
							Point root = null;
							final long partitionId = t._1()>>(mergingFactor*3);
							int furtherMerges = Math.min(levels-1, mergingFactor);
							
							//Initialize Hashmaps and Lists to store assigned point
							ArrayList<HashMap<Long,Point>> lods = new ArrayList<HashMap<Long,Point>>(furtherMerges);
							ArrayList<ArrayList<HashMap<Long,Point>>> splitLods = null;
							
							for(int i=1;i<=furtherMerges;i++) {
								lods.add(new HashMap<Long, Point>((int) Math.pow(Math.pow(2, i), 3)*2));
							}
							//In this case more Hashmaps has to be generated per level to keep the maximum point number per node
							if(levels-1>mergingFactor) {
								splitLods = new ArrayList<ArrayList<HashMap<Long,Point>>>(levels-mergingFactor-1);
								for(int i=furtherMerges+1;i<levels;i++) {
									int splitCount = (int) Math.pow(Math.pow(2, i-furtherMerges), 3);
									ArrayList<HashMap<Long,Point>> splitedMaps = 
											new ArrayList<HashMap<Long,Point>>(splitCount);
									
									//Initialize each split map inside cell
									for(long ii = 0; ii<splitCount;ii++) {
										splitedMaps.add(new HashMap<Long,Point>((int) Math.pow(Math.pow(2, furtherMerges), 3)*2));
									}
									splitLods.add(splitedMaps);
									
								}
								
							}
							
							//Start LoD calculation for each point
							Iterator<Point> it = t._2.iterator();
							outerloop:
							while(it.hasNext()) {
								Point aktP = it.next();
								if(root==null) {
									root=aktP;
									continue;
								}
								int i = 0;
								while(i<furtherMerges) {
									long aktid=aktP.getId()>>(shift-(3*(i+1))); //calculate id of virtual cell 
								if(!lods.get(i).containsKey(aktid)) {
									lods.get(i).put(aktid, aktP); // add point to level if virt. cell is not occupied
									continue outerloop;
								}
								i++;
								}
								// this loop is used for all levels with split hashmaps per level
								while(i<levels-1) {
									long aktid=aktP.getId()&shiftedValueMask;
									//id which points to the Hashmap responsible for the region in the given level of the given point
									// => id without last n bits and without bits before "shift"
									int hashid =(int) (aktid>>(shift-(3*(i-furtherMerges+1)))); 
									long cellid = aktid>>(shift-3*(i+1)) ; //Cell id inside Hashmap
									if(!splitLods.get(i-furtherMerges).get(hashid).containsKey(cellid)) {
										splitLods.get(i-furtherMerges).get(hashid).put(cellid,aktP);// add point to level if virt. cell is not occupied
										continue outerloop;
									}
									i++;
									}
							}
							
							//After LoD calculation generate a list of patches, where each patch represents one (partial) level of the 
							//final nested octree grid
							ArrayList<Tuple2<Long, Patch>> result = new ArrayList<Tuple2<Long, Patch>>();
							long patchID = Helper.calculateFinalIDForMerges(0, levels, mergingFactor,t._1); //calculate NodeId
							result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,root)));
							
							int l = 0;
							while(l<furtherMerges) {
								Collection<Point> points = lods.get(l).values();
								if(!points.isEmpty()) {
									patchID = Helper.calculateFinalIDForMerges(l+1, levels, mergingFactor,t._1);
									result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,points)));
								}
								l++;
							}
							while(l<levels-1) {
								int z = l+1;// actual level
								int i = 0;
								for(HashMap<Long,Point> pair:splitLods.get(l-furtherMerges)) {
									Collection<Point> points = pair.values();
									if(!points.isEmpty()) {
										long finalID = Helper.calculateFinalIDForSplits(z,(l-furtherMerges+1), levels,t._1(), i); //calculate NodeId
										result.add(new Tuple2<Long,Patch>(finalID,new Patch(finalID,partitionId,points)));
									}
									i++;
									}
								l++;
							}
							return result;
						}
						};
			
			// This function calculates the level of detail for all points in the given spacing-cell.
			// In this function the center point from each virtual cell will be chosen
			PairFlatMapFunction <Tuple2<Long,Iterable<PointExtended>>,Long,Patch> calcLODCellCenter = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<PointExtended>>,Long,Patch>(){

						@Override
						public Iterable<Tuple2<Long, Patch>> call(Tuple2<Long, Iterable<PointExtended>> t) throws Exception {
							PointExtended root = null;
							final long partitionId = t._1()>>(mergingFactor*3);
							int furtherMerges = Math.min(levels-1, mergingFactor);
							
							//Initialize Hashmaps and Lists to store assigned point
							ArrayList<HashMap<Long,PointExtended>> lods = new ArrayList<HashMap<Long,PointExtended>>(furtherMerges);
							ArrayList<ArrayList<HashMap<Long,PointExtended>>> splitLods = null;
							
							for(int i=1;i<=furtherMerges;i++) {
								lods.add(new HashMap<Long, PointExtended>((int) Math.pow(Math.pow(2, i), 3)*2));
							}
							//In this case more Hashmaps has to be generated per level to keep the maximum point number per node
							if(levels-1>mergingFactor) {
								splitLods = new ArrayList<ArrayList<HashMap<Long,PointExtended>>>(levels-mergingFactor-1);
								for(int i=furtherMerges+1;i<levels;i++) {
									int splitCount = (int) Math.pow(Math.pow(2, i-furtherMerges), 3);
									ArrayList<HashMap<Long,PointExtended>> splitedMaps = 
											new ArrayList<HashMap<Long,PointExtended>>(splitCount);
									
									//Initialize each split map inside cell
									for(long ii = 0; ii<splitCount;ii++) {
										splitedMaps.add(new HashMap<Long,PointExtended>((int) Math.pow(Math.pow(2, furtherMerges), 3)*2));
									}
									splitLods.add(splitedMaps);
									}
								}
							
							//Start LoD calculation for each point
							Iterator<PointExtended> it = t._2.iterator();
							int cellCenterX,cellCenterY,cellCenterZ;
							int centerShift;
							double dist;
							PointExtended poi;
							outerloop:
							while(it.hasNext()) {
								PointExtended aktP = it.next();
								// calculations for the root level
								if(root==null) {
									// calculate the cell center coordinates
									centerShift=(shift/3)-1; 
									cellCenterX=aktP.x>>centerShift;
									aktP.xCellCenter=cellCenterX<<centerShift;
									cellCenterY=aktP.y>>centerShift;
									aktP.yCellCenter=cellCenterY<<centerShift;
									cellCenterZ=aktP.z>>centerShift;
									aktP.zCellCenter=cellCenterZ<<centerShift;
									// calculate the distance of the given point to the cell center
									aktP.distance=Helper.distance(aktP.x, aktP.y, aktP.z, aktP.xCellCenter, aktP.yCellCenter, aktP.zCellCenter);
									root=aktP;
									continue;
								}
								
								else {
									poi = root;
									// calculate the distance from the given point to the cell center 
									dist=Helper.distance(aktP.x, aktP.y, aktP.z, poi.xCellCenter,poi.yCellCenter,poi.zCellCenter);
									// swap if given point is nearer than the old one
									if(dist<poi.distance) {
										aktP.distance=dist;
										aktP.xCellCenter=poi.xCellCenter;
										aktP.yCellCenter=poi.yCellCenter;
										aktP.zCellCenter=poi.zCellCenter;
										root=aktP;
										aktP=poi;
									}
								}
								int i = 0;
								while(i<furtherMerges) {
									long aktid=aktP.getId()>>(shift-(3*(i+1))); //calculate id of virtual cell
									
									//if cell is not occupied, assign actual point to cell an calculate its distance to cell center
									if(!lods.get(i).containsKey(aktid)) {
										centerShift=(shift/3)-i-2;
										cellCenterX=aktP.x>>centerShift;
										aktP.xCellCenter=cellCenterX<<centerShift;
										cellCenterY=aktP.y>>centerShift;
										aktP.yCellCenter=cellCenterY<<centerShift;
										cellCenterZ=aktP.z>>centerShift;
										aktP.zCellCenter=cellCenterZ<<centerShift;
										aktP.distance=Helper.distance(aktP.x, aktP.y, aktP.z, aktP.xCellCenter, aktP.yCellCenter, aktP.zCellCenter);
										lods.get(i).put(aktid, aktP);
										continue outerloop;
									}
									//if cell is occupied, check distances to cell center and assign the nearest point
									else {
										poi = lods.get(i).get(aktid);
										dist=Helper.distance(aktP.x, aktP.y, aktP.z, poi.xCellCenter,poi.yCellCenter,poi.zCellCenter);
										if(dist<poi.distance) {
											aktP.distance=dist;
											aktP.xCellCenter=poi.xCellCenter;
											aktP.yCellCenter=poi.yCellCenter;
											aktP.zCellCenter=poi.zCellCenter;
											lods.get(i).put(aktid, aktP);
											aktP=poi;
										}
									}
									i++;
								}
								//Same procedure in case of split Regions per Level
								while(i<levels-1) {
									long aktid=aktP.getId()&shiftedValueMask;
									//id which points to the Hashmap responsible for the region in the given level of the given point
									// => id without last n bits and without bits before "shift"
									int hashid =(int) (aktid>>(shift-(3*(i-furtherMerges+1))));
									long cellid = aktid>>(shift-3*(i+1)) ; //virtl. cell id inside Hashmap
									if(!splitLods.get(i-furtherMerges).get(hashid).containsKey(cellid)) {
										centerShift=(shift/3)-i-2;
										cellCenterX=aktP.x>>centerShift;
										aktP.xCellCenter=cellCenterX<<centerShift;
										cellCenterY=aktP.y>>centerShift;
										aktP.yCellCenter=cellCenterY<<centerShift;
										cellCenterZ=aktP.z>>centerShift;
										aktP.zCellCenter=cellCenterZ<<centerShift;
										aktP.distance=Helper.distance(aktP.x, aktP.y, aktP.z, aktP.xCellCenter, aktP.yCellCenter, aktP.zCellCenter);
										
										splitLods.get(i-furtherMerges).get(hashid).put(cellid,aktP);
										continue outerloop;
								}
								else {
									poi = splitLods.get(i-furtherMerges).get(hashid).get(cellid);
									dist=Helper.distance(aktP.x, aktP.y, aktP.z, poi.xCellCenter,poi.yCellCenter,poi.zCellCenter);
									if(dist<poi.distance) {
										aktP.distance=dist;
										aktP.xCellCenter=poi.xCellCenter;
										aktP.yCellCenter=poi.yCellCenter;
										aktP.zCellCenter=poi.zCellCenter;
										splitLods.get(i-furtherMerges).get(hashid).put(cellid,aktP);
										aktP=poi;
									}
								}
									i++;
								}
							}
							
							//After LoD calculation generate a list of patches, where each patch represents one (partial) level of the 
							//final nested octree grid
							ArrayList<Tuple2<Long, Patch>> result = new ArrayList<Tuple2<Long, Patch>>();
							
							long patchID = Helper.calculateFinalIDForMerges(0, levels, mergingFactor,t._1);
							result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,root))); 
							
							int l = 0;
							while(l<furtherMerges) {
								Collection<? extends Point> points = lods.get(l).values();
								if(!points.isEmpty()) {
									patchID = Helper.calculateFinalIDForMerges(l+1, levels, mergingFactor,t._1); //calculate NodeId
									result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,points)));
								}
								
								l++;
							}
							while(l<levels-1) {
								int z = l+1;// actual Level
								int i = 0;
								for(HashMap<Long,PointExtended> pair:splitLods.get(l-furtherMerges)) {
									Collection<? extends Point> points = pair.values();
									if(!points.isEmpty()) {
										long finalID = Helper.calculateFinalIDForSplits(z,(l-furtherMerges+1), levels,t._1(), i); //calculate NodeId
										result.add(new Tuple2<Long,Patch>(finalID,new Patch(finalID,partitionId, points)));
									}
									i++;
									}
								l++;
								}
							return result;
							}
				};
						
			
			// This function calculates the level of detail for all points in the given spacing-cell.
			// This function chooses for each level 2^level^3 random points
			PairFlatMapFunction <Tuple2<Long,Iterable<Point>>,Long,Patch> calcLODRandom = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<Point>>,Long,Patch>(){

						@Override
						public Iterable<Tuple2<Long, Patch>> call(Tuple2<Long, Iterable<Point>> t) throws Exception 
						{
							Point root = null;
							final long partitionId = t._1()>>(mergingFactor*3);
							int furtherMerges = Math.min(levels-1, mergingFactor);
							
							//Initialize Lists to store assigned point
							ArrayList<ArrayList<Point>> lods = new ArrayList<ArrayList<Point>>(furtherMerges);
							ArrayList<ArrayList<ArrayList<Point>>> splitLods = null;
							
							for(int i=1;i<=furtherMerges;i++) {
								lods.add(new ArrayList<Point>((int) Math.pow(Math.pow(2, i), 3)));
							}
							//In this case more lists has to be generated per level to keep the maximum point number per node
							if(levels-1>mergingFactor) {
								splitLods = new ArrayList<ArrayList<ArrayList<Point>>>(levels-mergingFactor-1);
								for(int i=furtherMerges+1;i<levels;i++) {
									int splitCount = (int) Math.pow(Math.pow(2, i-furtherMerges), 3);
									ArrayList<ArrayList<Point>> splitMaps = 
											new ArrayList<ArrayList<Point>>(splitCount);
									
									//Initialize each split map inside cell
									for(long ii = 0; ii<splitCount;ii++) {
										splitMaps.add(new ArrayList<Point>());
									}
									splitLods.add(splitMaps);
								}
							}
							int next;
							ArrayList<Point> points = new ArrayList<Point>((Collection<Point>)t._2); //create ArrayList of all actual Points
							next=ThreadLocalRandom.current().nextInt(points.size()); // chose one random point for root level
							// add to result and remove it from remaining points
							root = points.get(next);
							points.remove(next);
							int i = 0;
							int numOPoints;
							Point nextP;
							int maxNum=(int) Math.pow(8, furtherMerges); // maximum allowed numbers per node of the nested octree
							while(i<furtherMerges) {
								numOPoints=(int) Math.pow(8, i+1); // number of points from the actual level
								//chose "numOPoint" random points as points from the actual level
								for(int ii = 0;ii<numOPoints&&(!points.isEmpty());ii++){
									next=ThreadLocalRandom.current().nextInt(points.size());
									lods.get(i).add(points.get(next));
									points.remove(next);
								}
								i++;
								}
							
							// case of split regions per level
							while(i<levels-1) {
								numOPoints=(int) Math.pow(8, i+1);
								for(int ii = 0; ii<numOPoints&&(!points.isEmpty());ii++) {
									next=ThreadLocalRandom.current().nextInt(points.size());//determine random point
									nextP = points.get(next);
									long aktid=nextP.getId()&shiftedValueMask;
									int hashid =(int) (aktid>>(shift-(3*(i-furtherMerges+1))));//calculate region of given point
									// add this point if region is not full
									if(splitLods.get(i-furtherMerges).get(hashid).size()<maxNum) {
									splitLods.get(i-furtherMerges).get(hashid).add(nextP);
									points.remove(next);
								}
								
								}
								i++;
								
							}
							
							//After LoD calculation generate a list of patches, where each patch represents one (partial) level of the 
							//final nested octree grid
							ArrayList<Tuple2<Long, Patch>> result = new ArrayList<Tuple2<Long, Patch>>();
							
							long patchID = Helper.calculateFinalIDForMerges(0, levels, mergingFactor,t._1); //calculate NodeId
							result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,root)));
							
							int l = 0;
							while(l<furtherMerges) {
								if(!lods.get(l).isEmpty()) {
									patchID = Helper.calculateFinalIDForMerges(l+1, levels, mergingFactor,t._1); //calculate NodeId
									//result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,lods.get(l))));
									result.add(new Tuple2<Long,Patch>(patchID,new Patch(l+1,partitionId,lods.get(l))));
								}
								l++;
							}
							
							while(l<levels-1) {
								int z = l+1;
								i = 0;
								for(ArrayList<Point> pair:splitLods.get(l-furtherMerges)) {
									if(!pair.isEmpty()) {
										long finalID = Helper.calculateFinalIDForSplits(z,(l-furtherMerges+1), levels,t._1(), i); //calculate NodeId
										result.add(new Tuple2<Long,Patch>(finalID,new Patch(finalID,partitionId,pair)));
									}
									i++;
									}
								l++;
							}
							return result;
						}
				};
						
						
			// This function calculates the level of detail for all points in the given spacing-cell.
			// In a first step this function chooses for each level the first 2^level^3 points. In a next step these points are randomly
			// changed. Due to bad visual results this function is not used and deprecated.
			PairFlatMapFunction <Tuple2<Long,Iterable<Point>>,Long,Patch> calcLODRandomDeprecated = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<Point>>,Long,Patch>(){

						@Override
						public Iterable<Tuple2<Long, Patch>> call(Tuple2<Long, Iterable<Point>> t) throws Exception 
						{
							Point root = null;
							final long partitionId = t._1()>>(mergingFactor*3);
							int furtherMerges = Math.min(levels-1, mergingFactor);
							ArrayList<ArrayList<Point>> lods = new ArrayList<ArrayList<Point>>(furtherMerges);
							ArrayList<ArrayList<ArrayList<Point>>> splitLods = null;
							
							for(int i=1;i<=furtherMerges;i++) {
								lods.add(new ArrayList<Point>((int) Math.pow(Math.pow(2, i), 3)));
							}
							if(levels-1>mergingFactor) {
								splitLods = new ArrayList<ArrayList<ArrayList<Point>>>(levels-mergingFactor-1);
								for(int i=furtherMerges+1;i<levels;i++) {
									int splitCount = (int) Math.pow(Math.pow(2, i-furtherMerges), 3);
									ArrayList<ArrayList<Point>> splitMaps = 
											new ArrayList<ArrayList<Point>>(splitCount);
									for(long ii = 0; ii<splitCount;ii++) {
										splitMaps.add(new ArrayList<Point>());
									}
									splitLods.add(splitMaps);
									}
								}
							final int size = ((Collection<Point>)t._2).size();
							Iterator<Point> it = t._2.iterator();
							Point intermed;
							outerloop:
							while(it.hasNext()) {
								Point aktP = it.next();
								int random = ThreadLocalRandom.current().nextInt(size);
								if(root==null) {
									root=aktP;
									continue;
								}
								else {
									if(random==0) {
										intermed = root;
										root=aktP;
										aktP=intermed;
									}
								}
								int i = 0;
								int numOPoints=-1;
								while(i<furtherMerges) {
									numOPoints=(int) Math.pow(8, i+1);
									if(!(lods.get(i).size()==numOPoints)) {
										lods.get(i).add(aktP);
										continue outerloop;
									}
									else {
										if(random<=numOPoints*3) {
											int swapedPoint = random%numOPoints;
											intermed = lods.get(i).get(swapedPoint);
											lods.get(i).remove(swapedPoint);
											lods.get(i).add(aktP);
											aktP=intermed;
										}
											 
									}
								i++;
								}
								
								while(i<levels-1) {
									long aktid=aktP.getId()&shiftedValueMask;
									int hashid =(int) (aktid>>(shift-(3*(i-furtherMerges+1)))); 
									if(!(splitLods.get(i-furtherMerges).get(hashid).size()==numOPoints)) {
										splitLods.get(i-furtherMerges).get(hashid).add(aktP);
										continue outerloop;
								}
								else {
									if(random<=numOPoints*3) {
										int swapedPoint = random%numOPoints;
										intermed = splitLods.get(i-furtherMerges).get(hashid).get(swapedPoint);
										splitLods.get(i-furtherMerges).get(hashid).remove(swapedPoint);
										splitLods.get(i-furtherMerges).get(hashid).add(aktP);
										aktP=intermed;
										}
									}
								i++;
								}
							}
							ArrayList<Tuple2<Long, Patch>> result = new ArrayList<Tuple2<Long, Patch>>();
							
							long patchID = Helper.calculateFinalIDForMerges(0, levels, mergingFactor,t._1);
							result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,root))); //bei patch mit interleave id
							
							int l = 0;
							while(l<furtherMerges) {
								if(!lods.get(l).isEmpty()) {
									patchID = Helper.calculateFinalIDForMerges(l+1, levels, mergingFactor,t._1);
									result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,lods.get(l))));
								}
								l++;
							}
							while(l<levels-1) {
								int z = l+1;
								int i = 0;
								
								for(ArrayList<Point> pair:splitLods.get(l-furtherMerges)) {
									if(!pair.isEmpty()) {
										long finalID = Helper.calculateFinalIDForSplits(z,(l-furtherMerges+1), levels,t._1(), i);
										result.add(new Tuple2<Long,Patch>(finalID,new Patch(finalID,partitionId,pair)));
									}
									i++;
									}
								l++;
							}
							return result;
						}
			};
			
			
			
			// This function calculates the level of detail for all points in the given spacing-cell.
			// This function was implemented for test issues and chooses for each level the first 2^level^3 points.
			// Due to bad visual results this function is not used and deprecated.
			PairFlatMapFunction <Tuple2<Long,Iterable<Point>>,Long,Patch> calcLODRandomDeprecated2 = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<Point>>,Long,Patch>(){

						@Override
						public Iterable<Tuple2<Long, Patch>> call(Tuple2<Long, Iterable<Point>> t) throws Exception 
						{
							Point root = null;
							final long partitionId = t._1()>>(mergingFactor*3);
							int furtherMerges = Math.min(levels-1, mergingFactor);
							ArrayList<ArrayList<Point>> lods = new ArrayList<ArrayList<Point>>(furtherMerges);
							ArrayList<ArrayList<ArrayList<Point>>> splitLods = null;
							
							for(int i=1;i<=furtherMerges;i++) {
								lods.add(new ArrayList<Point>((int) Math.pow(Math.pow(2, i), 3)));
							}
							if(levels-1>mergingFactor) {
								splitLods = new ArrayList<ArrayList<ArrayList<Point>>>(levels-mergingFactor-1);
								for(int i=furtherMerges+1;i<levels;i++) {
									int splitCount = (int) Math.pow(Math.pow(2, i-furtherMerges), 3);
									ArrayList<ArrayList<Point>> splitMaps = 
											new ArrayList<ArrayList<Point>>(splitCount);
									
									//Initialize each split map inside cell
									for(long ii = 0; ii<splitCount;ii++) {
										splitMaps.add(new ArrayList<Point>());
									}
									splitLods.add(splitMaps);
									}
								}
							
							Iterator<Point> it = t._2.iterator();
							root = it.next();
							Point aktP;
							int i = 0;
							int numOPoints=8;
							int maxNum=(int) Math.pow(8, furtherMerges);
							outerloop:
							while(it.hasNext()) {
								if(i<furtherMerges) {
									aktP = it.next();
									lods.get(i).add(aktP);
									if(lods.get(i).size()==numOPoints) {
										i++;
										if(i==(levels-1)) break outerloop;
										numOPoints=(int)Math.pow(8, i+1);
									}
									}
								
								
								while(i>=furtherMerges) {
									aktP=it.next();
									long aktid=aktP.getId()&shiftedValueMask;
									int hashid =(int) (aktid>>(shift-(3*(i-furtherMerges+1))));
									if(splitLods.get(i-furtherMerges).get(hashid).size()<maxNum) {
										splitLods.get(i-furtherMerges).get(hashid).add(aktP);
										i=furtherMerges;
										continue outerloop;
								}
								
									i++;
									if(i==levels-1) {
										i=furtherMerges;
										break;
									}
								}
							}
							
							ArrayList<Tuple2<Long, Patch>> result = new ArrayList<Tuple2<Long, Patch>>();
							
							long patchID = Helper.calculateFinalIDForMerges(0, levels, mergingFactor,t._1);
							result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,root))); //bei patch mit interleave id
							
							int l = 0;
							while(l<furtherMerges) {
								if(!lods.get(l).isEmpty()) {
									patchID = Helper.calculateFinalIDForMerges(l+1, levels, mergingFactor,t._1);
									result.add(new Tuple2<Long,Patch>(patchID,new Patch(patchID,partitionId,lods.get(l))));
								}
								l++;
							}
							
							while(l<levels-1) {
								int z = l+1;
								i = 0;
								
								for(ArrayList<Point> pair:splitLods.get(l-furtherMerges)) {
									if(!pair.isEmpty()) {
										long finalID = Helper.calculateFinalIDForSplits(z,(l-furtherMerges+1), levels,t._1(), i);
										result.add(new Tuple2<Long,Patch>(finalID,new Patch(finalID,partitionId,pair)));
									}
									i++;
									}
								l++;
							}
							return result;
						}
				};
			
			
			
			// This function calculates the level of detail for all points in leaf nodes for the bottom-up approach.
			// This function was implemented for test issues and due to bad performance results this function is not used and deprecated.
			PairFlatMapFunction <Tuple2<Long,Iterable<Point>>,Long,IntermedPatch> calcLeafs = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<Point>>,Long,IntermedPatch>(){

						@Override
						public Iterable<Tuple2<Long, IntermedPatch>> call(Tuple2<Long, Iterable<Point>> t) throws Exception 
						{
							//Take the first 10000 points for a node
							ArrayList<Point> points = new ArrayList<Point>(10000);
							ArrayList<Tuple2<Long,IntermedPatch>>result = new ArrayList<Tuple2<Long,IntermedPatch>>();
							int i = 0;
							Iterator<Point> it = t._2.iterator();
							while(it.hasNext()&&i<10000) {
								Point p = it.next();
								points.add(p);
								i++;
							}
							long finalID = Helper.calculateFinalIDBoUp(levels-1, levels, t._1());//calculate Node ID
							result.add(new Tuple2<Long,IntermedPatch>(t._1>>3,new IntermedPatch(finalID,t._1()>>((levels-1)*3),points,levels-1)));
							return result;
						}
				
			};
			
			// This function calculates the level of detail for all points in inner nodes for the bottom-up approach.
			// This function was implemented for test issues and due to bad performance results this function is not used and deprecated.
			PairFlatMapFunction <Tuple2<Long,Iterable<IntermedPatch>>,Long,IntermedPatch> calcInner = 
					new PairFlatMapFunction<Tuple2<Long,Iterable<IntermedPatch>>,Long,IntermedPatch>(){

						@Override
						public Iterable<Tuple2<Long, IntermedPatch>> call(Tuple2<Long, Iterable<IntermedPatch>> t) throws Exception 
						{
							// Take a total of 10000 points from all child nodes
							ArrayList<Point> points = new ArrayList<Point>(10000);
							ArrayList<Tuple2<Long,IntermedPatch>>result = new ArrayList<Tuple2<Long,IntermedPatch>>();
							int numFromChild = 10000/((Collection<IntermedPatch>)t._2).size(); 
							int aktLod=-1;
							
							//Take for each child node the first "10000/Childs" points
							Iterator<IntermedPatch> it = t._2.iterator();
							while(it.hasNext()) {
								IntermedPatch p = it.next();
								int size = p.points.size();
								for(int i = 0; i<numFromChild&&i<size; i++) {
									points.add(p.points.get(0));
									p.points.remove(0);
									
								}
								//after taking the maximum number of points from child, prepare remaining points of child node
								//for writing into cassandra database
								if(!p.points.isEmpty()) {
									p.result=true;
									result.add(new Tuple2<Long,IntermedPatch>(0L,p));
								}
								aktLod=p.level-1;
							}
	
							long finalID = Helper.calculateFinalIDBoUp(aktLod, levels, t._1());// calculate node id of actual node
							result.add(new Tuple2<Long,IntermedPatch>(t._1>>3,new IntermedPatch(finalID,t._1()>>(aktLod*3),points,aktLod)));
							
							return result;
						}
				
			};
			
			// This functions filters after each iteration all patches which represent a final result. Only used in the bottom-up approach.
			Function<Tuple2<Long, IntermedPatch>, Boolean> filterfinal= new Function<Tuple2<Long, IntermedPatch>, Boolean>() {

				@Override
				public Boolean call(Tuple2<Long, IntermedPatch> v1) throws Exception {
					return v1._2.result;
				}
				
			};
			
			// This functions filters after each iteration all patches which not represent a final result. I.E. all patches which will be used
			// in the next iteration for determining the points of the higher level. Only used in the bottom-up approach.
			Function<Tuple2<Long, IntermedPatch>, Boolean> nextIter= new Function<Tuple2<Long, IntermedPatch>, Boolean>() {

				@Override
				public Boolean call(Tuple2<Long, IntermedPatch> v1) throws Exception {
					return !v1._2.result;
				}
				
			};
			
			
			// This function transforms a IntermedPatch to a final Patch. Only used in the bottom-up approach.
			Function<IntermedPatch, Patch> writablePatches= new Function<IntermedPatch, Patch>() {

				@Override
				public Patch call(IntermedPatch v1) throws Exception {
					return new Patch(v1.cellid,v1.partitionid,v1.points);
				}
				
			};
			
			// This function is used for transforming a read Node from Cassandra Database into a patch for further processing steps
			PairFlatMapFunction<List,Long,Point> flatListToPoint = new PairFlatMapFunction<List,Long,Point>() {

				@Override
				public Iterable<Tuple2<Long, Point>> call(List t) throws Exception {
					List<Tuple2<Long,Point>> result= new ArrayList<Tuple2<Long,Point>>();
					for(Object o : t) {
						byte[] b = (byte[]) o;
			        	int x = Helper.byteToInt(Arrays.copyOfRange(b, 0, 4));
			        	int y = Helper.byteToInt(Arrays.copyOfRange(b, 4, 8));
			        	int z = Helper.byteToInt(Arrays.copyOfRange(b, 8, 12));
			        	
						long id= Helper.magicBitMorton(x,y,z,truncate);
						
						result.add(new Tuple2<Long,Point>(id>>shift,new Point(id,b)));
					}
					return result;
				}

				};
				
				// This function is used for transforming a read Node from Cassandra Database into a patch for further processing steps
				// with the cell center sampling
				PairFlatMapFunction<List,Long,PointExtended> flatListToPointExtended = new PairFlatMapFunction<List,Long,PointExtended>() {

					@Override
					public Iterable<Tuple2<Long, PointExtended>> call(List t) throws Exception {
						List<Tuple2<Long,PointExtended>> result= new ArrayList<Tuple2<Long,PointExtended>>();
						for(Object o : t) {
							byte[] b = (byte[]) o;
				        	int x = Helper.byteToInt(Arrays.copyOfRange(b, 0, 4));
				        	int y = Helper.byteToInt(Arrays.copyOfRange(b, 4, 8));
				        	int z = Helper.byteToInt(Arrays.copyOfRange(b, 8, 12));
				        	
							long id= Helper.magicBitMorton(x,y,z,truncate);
							
							result.add(new Tuple2<Long,PointExtended>(id>>shift,new PointExtended(id,b,x,y,z)));
						}
						return result;
					}

					};
					
					// This function transforms a read database entry into a List of byte-arrays representing the point properties
					final class mapToListRdd extends AbstractFunction1<Row, List> implements Serializable{

						public List apply(Row r) {
							return JavaConversions.seqAsJavaList((Seq<byte[]>) r.get(2));
						}
					
						
					}

					
					// This function is used to compine patches into one
					Function2<Patch,Patch,Patch> combinePatches = new Function2<Patch,Patch,Patch>(){

						@Override
						public Patch call(Patch p1, Patch p2) throws Exception {
							p1.addAllPoints(p2.getpoints());
							return p1;
						}
						
					};
		
							
						

		//===============================================================================================//
		//              This next part defines the Spark point cloud processing pipeline                 //
		//===============================================================================================//
			long start = System.currentTimeMillis();
			//https://dl.bintray.com/spark-packages/maven/IGNF/spark-iqmulus/0.1.0-s_2.10/
			DataFrame d = sqlc.read().format("fr.ign.spark.iqmulus.las").load(fileLocation);

			JavaRDD<List> indexedRddLists = null;
			
			if(addToRegion) {
				//normalizes min-coordintates from the given file in root bit accuracy. (Start point of database octree range query)
				int normMinX=Helper.normalize(minX,minOldx, scale)>>(spacing+mergingFactor)*3;
				int normMinY=Helper.normalize(minY, minOldy, scale)>>(spacing+mergingFactor)*3;
				int normMinZ=Helper.normalize(minZ, minOldz ,scale)>>(spacing+mergingFactor)*3;
				
				//Database query of the given range						
				DataFrame indexedData = sqlc.read().format("org.apache.spark.sql.cassandra").option("keyspace", keyspace)
		                .option("table", table).load().filter(Helper.calculateRangeQuery(normMinX, normMinY, normMinZ, 
		                		normMaxX>>(spacing+mergingFactor)*3, normMaxY>>(spacing+mergingFactor)*3, 
					normMaxZ>>(spacing+mergingFactor)*3));
				//transform dataframe to rdd of lists with byte-arrays
				indexedRddLists = indexedData.map(new mapToListRdd(), scala.reflect.ClassManifestFactory.fromClass(List.class)).toJavaRDD();
			}

			//top-down approaches
			if(dir==0) {
				
				JavaPairRDD<Long,Patch> lodsgroup=null;
				//cell center sampling 
				if(mode==0) {
					//calculate morton-code and tranform into JavaRDD
					JavaRDD<Tuple2<Long,PointExtended>> row2RDD= d.map(new calcIndexExtended(),
							scala.reflect.ClassManifestFactory.fromClass((Class<Tuple2<Long,PointExtended>>) (Class<?>)Tuple2.class)).toJavaRDD();
					
					//transform tuple into JavaPairRdd
					JavaPairRDD<Long,PointExtended> pair = JavaPairRDD.fromJavaRDD(row2RDD);
					System.out.println("Sampling: Center Point of Virtual Grid");
					//if points should be add into a indexed region, read this region from Database, transform points and add them into actual PairRdd
					if(addToRegion) {
						JavaPairRDD<Long,PointExtended> readPair = indexedRddLists.flatMapToPair(flatListToPointExtended);
						pair=pair.union(readPair);
					}
					
					//group into spacing cells and calculate levels for these points
					 lodsgroup = pair.groupByKey().flatMapToPair(calcLODCellCenter);
				}
				
				
				//other sampling strategies
				else {		
			
					//calculate morton-code and tranform into JavaRDD
					JavaRDD<Tuple2<Long,Point>> row2RDD= d.map(new calcIndex(),
						scala.reflect.ClassManifestFactory.fromClass((Class<Tuple2<Long,Point>>) (Class<?>)Tuple2.class)).toJavaRDD();
					
					//transform tuple into JavaPairRdd
					JavaPairRDD<Long,Point> pair = JavaPairRDD.fromJavaRDD(row2RDD);
					
					//if points should be add into a indexed region, read this region from Database, transform points and add them into actual PairRdd
					if(addToRegion) {
						JavaPairRDD<Long,Point> readPair = indexedRddLists.flatMapToPair(flatListToPoint);
						pair=pair.union(readPair);
					}
				
				
					//level calculation for First Point of Virtual Grid Sampling
					if(mode==1) {
						System.out.println("Sampling: First Point of Virtual Grid");
						 lodsgroup = pair.groupByKey().flatMapToPair(calcLODFirst);
					}
					
					//level calculation for Random Point Sampling
					if(mode==2) {
						 lodsgroup = pair.groupByKey().flatMapToPair(calcLODRandom);
						 System.out.println("Sampling: Random Points");
						 System.out.println(minX);
							System.out.println(minY);
							System.out.println(minZ);
							System.out.println(truncate);
					}
					
					//level calculation for deprecated Random Point Sampling
					if(mode==3) {
						System.out.println("Sampling: Random Points, DEPRECATED!");
						lodsgroup = pair.groupByKey().flatMapToPair(calcLODRandomDeprecated);
					}
					
					//level calculation for deprecated Random Point Sampling
					if(mode==4) {
						 lodsgroup = pair.groupByKey().flatMapToPair(calcLODRandomDeprecated2);
						 System.out.println("Sampling: Random Points, DEPRECATED!");
					}
				}
				
				//combine patches to one node
				JavaRDD<Patch> finalPatches = lodsgroup.reduceByKey(combinePatches).values();
			
				//write into Cassandra database
				CassandraJavaUtil.javaFunctions(finalPatches)
		        .writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(Patch.class)).saveToCassandra();
			}
			
			
			//bottom-up approach
			if(dir==1) {
				System.out.println("Bottom-Up Aproach, SLOW!");
				
				//calculate morton-code and tranform into JavaRDD for bottom-up approach
				JavaRDD<Tuple2<Long,Point>> row2RDD= d.map(new calcIndexBoUp(),
						scala.reflect.ClassManifestFactory.fromClass((Class<Tuple2<Long,Point>>) (Class<?>)Tuple2.class)).toJavaRDD();
				
				//transform into pairRDD
				JavaPairRDD<Long,Point> pair = JavaPairRDD.fromJavaRDD(row2RDD);
				
				//group points to leaf nodes and sample points
				JavaPairRDD<Long,IntermedPatch> lodsgroup = pair.groupByKey().flatMapToPair(calcLeafs);
				//bottom-up node calculation for each node until reaching the root nodes
				for(int i=levels-1;i>0;i--) {
					lodsgroup = lodsgroup.groupByKey().flatMapToPair(calcInner);
					if(i!=1) {
						//filter finished nodes
						JavaPairRDD<Long,IntermedPatch> finals = lodsgroup.filter(filterfinal);
						//write finished nodes into database
						JavaRDD<Patch> finalPatches = finals.values().map(writablePatches);
						 CassandraJavaUtil.javaFunctions(finalPatches)
					        .writerBuilder("ptab", "test", CassandraJavaUtil.mapToRow(Patch.class)).saveToCassandra();
						 lodsgroup=lodsgroup.filter(nextIter);
					 
					}
					
				}
				//write root nodes into database
				JavaRDD<Patch> finalPatches = lodsgroup.values().map(writablePatches);
				 CassandraJavaUtil.javaFunctions(finalPatches)
			        .writerBuilder(keyspace, table, CassandraJavaUtil.mapToRow(Patch.class)).saveToCassandra();
			}
			
			System.out.println("TOTAL TIME:"+(System.currentTimeMillis()-start)+"ms");
		}
	
	}

}
