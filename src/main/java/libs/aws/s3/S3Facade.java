package libs.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import libs.aws.s3.util.Util;

/**
 * This class communicates with {@link S3RequestHandler} (or other components)
 * and provides APIs for following -
 * 
 * Upload file to S3 - Automatically manages whether file should be uploaded
 * using multipart or normal single upload. Use
 * MEMORY_PERCENT_AVAILABLE_AFTER_FILE property to decide
 * 
 * Download file from S3 - Automatically manages whether file should be uploaded
 * using multipart or normal single upload. Use
 * MEMORY_PERCENT_AVAILABLE_AFTER_FILE property to decide
 * 
 * Adjust MEMORY_PERCENT_AVAILABLE_AFTER_FILE property according to requirement.
 * 
 * Use {@link S3RequestHandler} directly to make indenpendent calls
 * 
 * @author Kuldeep
 *
 */
public class S3Facade {

	/**
	 * Change this value if you want to modify percent settings
	 * {@link Util .checkPercentAvaiableMemoryAfterSize}. This value is used to
	 * determine whether file should be uploaded multipart or not as well to
	 * download file in response or in target file path, with this setting you
	 * can mention what is expected percent of memory required to be free after
	 * occupying file size bytes. Extend the method
	 * {@link S3Facade#uploadObject(AmazonS3, String, String, File, ObjectMetadata)}
	 * to take this as parameter
	 */
	private final static int MEMORY_PERCENT_AVAILABLE_AFTER_FILE = 20;

	/**
	 * Similar as {@link S3Facade#MEMORY_PERCENT_AVAILABLE_AFTER_FILE} but uses
	 * available memory in bytes when {@link S3Facade#usePercent} option is off
	 */
	private final static int MEMORY_AVAILABLE_AFTER_FILE = 1024 * 1024 * 500; // 500
																				// MB

	/**
	 * Set to true to use {@link S3Facade#MEMORY_PERCENT_AVAILABLE_AFTER_FILE}
	 * property and percent based calculation. Set to false to use
	 * {@link S3Facade#MEMORY_AVAILABLE_AFTER_FILE} option
	 */
	private boolean usePercent = true;

	private final static S3Facade instance = new S3Facade();
	private S3RequestHandler requestHandler = new S3RequestHandler();

	public final static S3Facade instance() {
		return instance;
	}

	/**
	 * See
	 * 
	 * 
	 * @param s3Client
	 * @param bucket
	 * @param key
	 * @param filePath
	 * @param metaData
	 * @return
	 * @throws IOException
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 */
	public boolean uploadObject(AmazonS3 s3Client, String bucket, String key, String filePath, ObjectMetadata metaData)
			throws IOException, AmazonServiceException, AmazonClientException {
		return uploadObject(s3Client, bucket, key, new File(filePath), metaData);

	}

	/**
	 * Uploads file to S3 with provided client connection, bucket and key/file
	 * name. See {@link S3Facade#MEMORY_PERCENT_AVAILABLE_AFTER_FILE} or
	 * {@link S3Facade#MEMORY_AVAILABLE_AFTER_FILE} and
	 * {@link S3Facade#usePercent} flag
	 * 
	 * @param s3Client
	 *            - client connection object ( BasicAWSCredentials creds = new
	 *            BasicAWSCredentials(accessKey, secretKey); AmazonS3 s3Client =
	 *            AmazonS3ClientBuilder.standard().withCredentials(new
	 *            AWSStaticCredentialsProvider(creds)) .build(); )
	 * @param bucket
	 *            - remote bucket
	 * @param key
	 *            - remote file name/key in S3 terminology
	 * @param file
	 *            - source file
	 * @param metaData
	 *            - any user meta data. Automatically adds content length
	 * @return whether operation success or failed
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @throws IOException
	 */
	public boolean uploadObject(AmazonS3 s3Client, String bucket, String key, File file, ObjectMetadata metaData)
			throws AmazonServiceException, AmazonClientException, IOException {

		long fileLength = file.length();
		if (fileLength < 1) {
			return false;
		}
		boolean singleUpload = true;
		if (usePercent) {
			singleUpload = Util.checkPercentAvaiableMemoryAfterSize(MEMORY_PERCENT_AVAILABLE_AFTER_FILE, fileLength);
		} else {
			singleUpload = Util.checkAvaiableMemoryAfterSize(MEMORY_AVAILABLE_AFTER_FILE, fileLength);
		}
		if (singleUpload) {
			return requestHandler.uploadFile(s3Client, bucket, key, file, metaData);
		} else {
			return requestHandler.uploadMultipartFile(s3Client, bucket, key, file, metaData);
		}

	}

	/**
	 * Download/Returns object. Response uses the same logic as upload to
	 * whether download as S3 response object or store contents to file.
	 * Response automatically includes object's meta data
	 * 
	 * @param s3Client
	 *            - client connection object ( BasicAWSCredentials creds = new
	 *            BasicAWSCredentials(accessKey, secretKey); AmazonS3 s3Client =
	 *            AmazonS3ClientBuilder.standard().withCredentials(new
	 *            AWSStaticCredentialsProvider(creds)) .build(); )
	 * @param bucket
	 *            - remote s3 bucket
	 * @param file
	 *            - remote file
	 * @param destination
	 *            - destination file to store to. Ignored if output is part of
	 *            response object. See responseStoredToFile of
	 *            {@link S3DataResponse} flag
	 * @return {@link S3DataWithMetaDataResponse} - contains meta data, response
	 *         as well flag to verify if object is stored to destination or part
	 *         of current object
	 * @throws AmazonClientException
	 * @throws AmazonServiceException
	 * @throws IOException
	 */
	public S3DataWithMetaDataResponse getObject(AmazonS3 s3Client, String bucket, String file, File destination)
			throws AmazonClientException, AmazonServiceException, IOException {
		S3DataWithMetaDataResponse response = new S3DataWithMetaDataResponse();
		ObjectMetadata metaData = requestHandler.getObjectMetaData(s3Client, bucket, file);
		if (Util.checkPercentAvaiableMemoryAfterSize(MEMORY_PERCENT_AVAILABLE_AFTER_FILE,
				metaData.getContentLength())) {
			S3DataResponse s3Response = new S3DataResponse();
			response.setDataResponse(s3Response);
			s3Response.setS3Object(requestHandler.getObject(s3Client, bucket, file));

		} else {
			response.setDataResponse(requestHandler.getObject(s3Client, bucket, file, destination));
		}
		response.setMetaData(metaData);
		return response;
	}

	/**
	 * Main just to quick test
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		String accessKey = "", secretKey = "";
		Properties p = new Properties();
		p.load(new FileInputStream("../config.properties"));
		accessKey = p.getProperty("accessKey");
		secretKey = p.getProperty("secretKey");
		BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
				.build();
		
		  System.out.println(S3Facade.instance().uploadObject(s3Client,
		  "your-bucket", "bigfile.mp4", "g:\\test2.mp4", null));
		
		
	}
}
