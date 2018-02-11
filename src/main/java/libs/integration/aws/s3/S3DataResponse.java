package libs.integration.aws.s3;

import java.io.File;

import com.amazonaws.services.s3.model.S3Object;

/**
 * Response of load/download S3 object action
 * 
 * @author Kuldeep
 *
 */
public class S3DataResponse {

	/**
	 * S3 object, if response is object and can be stored in memory
	 */
	private S3Object s3Object;
	/**
	 * If true, S3 object is null and object copied to provided file
	 */
	private boolean responseStoredToFile = false;

	/**
	 * File where object is stored when
	 * {@link S3DataResponse#responseStoredToFile} is true
	 */
	private File file;

	public S3Object getS3Object() {
		return s3Object;
	}

	public void setS3Object(S3Object s3Object) {
		this.s3Object = s3Object;
	}

	public boolean isResponseStoredToFile() {
		return responseStoredToFile;
	}

	public void setResponseStoredToFile(boolean responseStoredToFile) {
		this.responseStoredToFile = responseStoredToFile;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

	@Override
	public String toString() {
		return "S3DataResponse [s3Object=" + s3Object + ", responseStoredToFile=" + responseStoredToFile + ", filePath="
				+ file + "]";
	}
}
