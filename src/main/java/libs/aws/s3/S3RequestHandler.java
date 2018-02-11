package libs.aws.s3;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.UploadPartRequest;

public class S3RequestHandler {

	/**
	 * Upload file as simple put request
	 * 
	 * @param s3Client
	 * @param bucketName
	 * @param remoteFileName
	 * @param file
	 * @param metaData
	 * @return
	 * @throws AmazonServiceException
	 * @throws IOException
	 */
	public boolean uploadFile(AmazonS3 s3Client, String bucketName,
			String remoteFileName/* key in s3 */, File file, ObjectMetadata metaData)
			throws AmazonServiceException, IOException {
		if (metaData == null) {
			metaData = new ObjectMetadata();
		}
		if (metaData.getContentLength() < 1) {
			metaData.setContentLength(file.length());
		}
		s3Client.putObject(bucketName, remoteFileName, new FileInputStream(file), metaData);
		// can utilize return for more specific handling
		return true;
	}

	/**
	 * Uploads file as multipart. To decide whether to use simple put object or
	 * multipart, use
	 * {@link S3Facade#uploadObject(AmazonS3, String, String, File, ObjectMetadata)}
	 * 
	 * @param s3Client
	 * @param bucketName
	 * @param remoteFileName
	 * @param file
	 * @param metaData
	 * @return
	 * @throws AmazonServiceException
	 * @throws IOException
	 */
	public boolean uploadMultipartFile(AmazonS3 s3Client, String bucketName,
			String remoteFileName/* key in s3 */, File file, ObjectMetadata metaData)
			throws AmazonServiceException, IOException {
		List<PartETag> partETags = new ArrayList<PartETag>();

		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, remoteFileName);
		InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
		try {
			long contentLength = file.length();
			long partSize = 1024 * 1024 * 10; // 10 mb blocks
			long filePosition = 0;
			long numberOfParts = contentLength / partSize;
			boolean extraBlock = false;
			long extraBlockValue = contentLength % partSize;
			if (extraBlockValue > 0) {
				extraBlock = true;
			}
			int i = 1;
			for (i = 1; i < numberOfParts; i++) {
				UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName)
						.withKey(remoteFileName).withUploadId(initResponse.getUploadId()).withObjectMetadata(metaData)
						.withPartNumber(i).withFileOffset(filePosition).withFile(file).withPartSize(partSize);
				filePosition += partSize;
				partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());
			}
			if (extraBlock) {
				UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName)
						.withKey(remoteFileName).withUploadId(initResponse.getUploadId()).withPartNumber(i)
						.withFileOffset(filePosition).withFile(file).withPartSize(extraBlockValue);

				partETags.add(s3Client.uploadPart(uploadRequest).getPartETag());

			}

			CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, remoteFileName,
					initResponse.getUploadId(), partETags);

			s3Client.completeMultipartUpload(compRequest);
		} catch (AmazonServiceException e) {

			s3Client.abortMultipartUpload(
					new AbortMultipartUploadRequest(bucketName, remoteFileName, initResponse.getUploadId()));
			throw e;
		}
		return true;
	}

	/**
	 * Returns object metadata using S3 client method call
	 * 
	 * @param s3Client
	 * @param bucket
	 * @param file
	 * @return
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 */
	public ObjectMetadata getObjectMetaData(AmazonS3 s3Client, String bucket, String file)
			throws AmazonServiceException, AmazonClientException {
		return s3Client.getObjectMetadata(bucket, file);
	}

	/**
	 * Returns object  using S3 client method call
	 * 
	 * @param s3Client
	 * @param bucket
	 * @param file
	 * @return
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 */
	public S3Object getObject(AmazonS3 s3Client, String bucket, String file)
			throws AmazonServiceException, AmazonClientException {

		return s3Client.getObject(bucket, file);
	}
	
	/**
	 * Stores object into file
	 * 
	 * @param s3Client
	 * @param bucket
	 * @param file
	 * @param destination
	 *            - file to store output to
	 * @return
	 * @throws AmazonServiceException
	 * @throws AmazonClientException
	 * @throws IOException
	 */
	public S3DataResponse getObject(AmazonS3 s3Client, String bucket, String file, File destination)
			throws AmazonServiceException, AmazonClientException, IOException {
		S3DataResponse response = new S3DataResponse();
		response.setResponseStoredToFile(true);
		GetObjectRequest request = new GetObjectRequest(bucket, file);
		s3Client.getObject(request, destination);
		response.setFile(destination);
		response.setS3Object(s3Client.getObject(bucket, file));
		return response;
	}

}
