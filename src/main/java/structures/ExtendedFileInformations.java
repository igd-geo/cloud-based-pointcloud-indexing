package structures;

import java.util.ArrayList;

//This class is used to store all relevant informations of a las-file. It is used for the progressive approach.
public class ExtendedFileInformations extends FileInformations {
	ArrayList<PCFile> remainingFiles;
	
	public ExtendedFileInformations(ArrayList<PCFile> remainingFiles, double minX, double minY, double minZ, double scale) {
		super(minX, minY, minZ,0,0,0, scale);
		this.remainingFiles=remainingFiles;
	}
	
	public ArrayList<PCFile> getRemainingFiles(){
		return this.remainingFiles;
	}
	

}
