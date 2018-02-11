package libs.integration.aws.s3.util;

/**
 * Some utility methods
 * 
 * @author Kuldeep
 *
 */
public class Util {

	/**
	 * @return true if available percent memory after occupying size is greater
	 *         than given percent
	 * @param percent
	 * 
	 * @param size
	 *            in bytes
	 * 
	 */
	public static boolean checkPercentAvaiableMemoryAfterSize(float percent, long size) {

		if (percent < 0) {
			return false;
		}
		long freeMemory = Runtime.getRuntime().freeMemory();
		if (freeMemory < size) {
			return false;
		}
		long extraMemory = freeMemory - size;
		if (((float) ((float) extraMemory / (float) Runtime.getRuntime().totalMemory()) * 100) < percent) {
			return false;
		}
		return true;
	}

	/**
	 * @return true if available memory after occupying size is greater than
	 *         given percent
	 * @param percent
	 * 
	 * @param size
	 *            in bytes
	 * 
	 */
	public static boolean checkAvaiableMemoryAfterSize(long free, long size) {

		long freeMemory = Runtime.getRuntime().freeMemory();
		if (freeMemory < size) {
			return false;
		}
		long extraMemory = freeMemory - size;
		if (extraMemory < free) {
			return false;
		}
		return true;
	}

}
