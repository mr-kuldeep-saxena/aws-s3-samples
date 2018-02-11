package libs.integration.aws.s3;

import com.amazonaws.services.s3.model.ObjectMetadata;

/**
 * Response object which contains meta data as well {@link S3DataResponse}
 * 
 * @author Kuldeep
 *
 */
public class S3DataWithMetaDataResponse {
	
	/**
	 * data response, See {@link S3DataResponse}
	 */
	private S3DataResponse dataResponse;
	/**
	 * Object meta data, user defined as well, s3 provided
	 */
	private ObjectMetadata metaData;
	
	
	public ObjectMetadata getMetaData() {
		return metaData;
	}
	public void setMetaData(ObjectMetadata metaData) {
		this.metaData = metaData;
	}
	public S3DataResponse getDataResponse() {
		return dataResponse;
	}
	public void setDataResponse(S3DataResponse dataResponse) {
		this.dataResponse = dataResponse;
	}
	@Override
	public String toString() {
		StringBuffer metaDataStr = new StringBuffer();
		metaDataStr.append("Raw Meta Data - \n\t");
		for (String raw:metaData.getRawMetadata().keySet()){
			metaDataStr.append(raw+":" + metaData.getRawMetadata().get(raw) + "\t");
		}
		metaDataStr.append("\n\tUser Meta Data - \n\t");
		for (String user:metaData.getUserMetadata().keySet()){
			metaDataStr.append(user+":" + metaData.getRawMetadata().get(user) + "\t");
		}
		return "S3DataWithMetaDataResponse [dataResponse=" + dataResponse + ",\nMetaData=\n\t" + metaDataStr + "]";
	}

}
