package libs.integration.aws.s3;

import java.util.ArrayList;
import java.util.Arrays;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.BucketCrossOriginConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Transition;
import com.amazonaws.services.s3.model.CORSRule;
import com.amazonaws.services.s3.model.CORSRule.AllowedMethods;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.SetBucketAclRequest;
import com.amazonaws.services.s3.model.SetBucketCrossOriginConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.model.lifecycle.LifecycleTagPredicate;

/**
 * In this class various S3 APIs are mentioned. Not all APIs sample code is
 * provided here because some of them are straight forward calls or not good to
 * be used from SDK (console is better for some operation). Some of concept's
 * details are presented within this section of doc.
 * 
 * 1. Bucket Create/Delete- {@link Misc#bucketOperations(AmazonS3)}
 * 
 * 2. Lifecycle Creation and Deletion {@link Misc#lifeCycleOperations(AmazonS3)}
 * 
 * 3. ACL {@link Misc#aclOperations(AmazonS3)}
 * 
 * 4.<i> Acceleration - Acceleration can be used to optimize speed of transfer.
 * When enabled it uses cloud front (edge) locations to transfer object from/to
 * client location. It can only be enabled or disabled and has cost when
 * enabled. Sample code is like - BucketAccelerateConfiguration config = new
 * BucketAccelerateConfiguration(BucketAccelerateStatus.Enabled);
 * s3Client.setBucketAccelerateConfiguration(bucket,
 * accelerateConfiguration); </i> 5. CORS - {@link Misc#corsOperations(AmazonS3}
 * 
 * 6. <i>Inventory Report - This can be enabled to get inventory reports of all
 * the objects in your bucket or matching given prefix/filter. Like -
 * SetBucketInventoryConfigurationRequest request = new
 * SetBucketInventoryConfigurationRequest(); InventoryConfiguration config = new
 * InventoryConfiguration().withEnabled(true) .withDestination(new
 * InventoryDestination() .withS3BucketDestination(new
 * InventoryS3BucketDestination().withBucketArn("arn"))) .withSchedule(new
 * InventorySchedule().withFrequency(InventoryFrequency.Weekly));
 * 
 * request.setInventoryConfiguration(config);
 * s3Client.setBucketInventoryConfiguration(request); </i> 7. <i>Bucket Logging
 * - You can enable bucket logging to track request to your bucket/object in
 * bucket. Programatically when you provide Target Bucket and Target prefix and
 * send that request, logging gets enabled. </i> 8. <i>Metrics enable - You can
 * enable metrics for bucket. There are 3 types of metrics Storage(Free) ,
 * Request(Paid), Data Transfer (Paid). You can set filter to capture metics for
 * a subset of data. Once enabled (after 15 mins or so), you can view metrics
 * data on Cloud watch. Basically metics are useful to analyze user access
 * pattern (Request metric), using which you can structure your data and move
 * unused data to other storage like S3 IA. </i> 9. <i>Tagging - You can use
 * Object tagging to group objects or locate objects. With prefix you do the
 * same things, but tags are more structured and allows you categorize object
 * irrespective of locations within bucket. </i> 10. <i>Verionning - This
 * enables you to keep multiple versions of an object in a bucket. Look at -
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/ObjectVersioning.html of how
 * it works. At API level you just need to set the status of versioning to
 * enable and send that request. </i> 11. <i>Website - You can use your S3
 * bucket to host static website. It is useful for small websites with no or
 * little dynamic behavior. You can also attach your domain name to S3 bucket
 * endpoint and it starts serving as website. For dynamic contents you can still
 * use AWS serverless services like API gateway and AWS lambda. By such
 * structure you don't need to manage any infrastructure. S3 -> Static Page
 * Serve with CSS, JS, HTML API. Gateway + Lambda -> API call from browser to do
 * dynamic action according to user input </i> 12. <i>Notification - S3 can be
 * integrated with other AWS services such as AWS lambda, in other words you can
 * trigger a lambda function when some event happens in bucket. Though it can be
 * configured programmatically, but most of time you will use console to
 * configure object, target and action of notification. </i>
 * 
 * 13. <i>Replication -
 * 
 * By default your bucket is replicated in different AZ in the region you
 * selected during bucket creation time. However, You can also enable cross -
 * region replication which replicates your data to different region as well.
 * Though you can recover data for any region failure but I am not a good fan of
 * this replication because of following -
 * 
 * No Automatic Failover, Manual disaster recovery - As you know every bucket
 * has unique url, there is no such thing as automatic failover to another
 * region in case of region failure. You have to rewrite your code/update URL in
 * case of these events. Data is already replicated to different AZs in same
 * region and when region fail (very rare this can happen) and you have to do
 * manual action in that case. It is not giving much value.
 * 
 * -- Versionning should be enabled for replication
 * 
 * -- When replicating you have to pay price for the target (bucket) replication
 * storage as well
 * 
 * -- Create time, meta data and ACLs are automatically replicated to
 * destination </i> <b>
 * 
 * Note - In S3 every object is immutable, so any update, modification,
 * transfer, move creates a new object </b>F
 * 
 * @author Kuldeep
 *
 */
public class Misc {

	String bucket = "bucket-";

	/**
	 * Basic Bucket operations
	 * 
	 * @param s3Client
	 */
	public void bucketOperations(AmazonS3 s3Client) {

		// -> Bucket creation

		// Request can't be anonymous access. It always an
		// authenticated/authorized user.
		/*
		 * use below option when you want to configure various properties at
		 * time of bucket creation. just to create bucket use direct string
		 * parameter method or with specified region method
		 */

		CreateBucketRequest request = new CreateBucketRequest(bucket);
		// set access control list/ACL
		request.setAccessControlList(null);
		// set region of bucket? - it is deprecated, use can update client to
		// specific region though
		s3Client.setRegion(Region.getRegion(Regions.AP_NORTHEAST_1));
		// similarly, lifecycle and other properties can be set on client
		// object after bucket created. like
		// s3Client.setBucketLifecycleConfiguration(null);

		s3Client.createBucket(request);
		// <-

		// -> Bucket Delete
		s3Client.deleteBucket("my-bucket-name-appear-here");

		// list buckets
		s3Client.setRegion(Region.getRegion(Regions.US_EAST_1));
		s3Client.listBuckets();
	}

	/**
	 * Life cycle CRUD
	 * 
	 * @param s3Client
	 */
	public void lifeCycleOperations(AmazonS3 s3Client) {

		// Note - Any kind of move, transfer, copy, etc. will always reset the
		// creation date of the object.

		// samples example rules from aws sdk
		BucketLifecycleConfiguration.Rule rule1 = new BucketLifecycleConfiguration.Rule()
				.withId("move older file with 2015-01-01 prefix")
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(
						"2015-01-01/")))/*
										 * file with prefix of 2015, Jan 01/,
										 * move to glacier
										 */
				.addTransition(new Transition().withDays(0).withStorageClass(StorageClass.Glacier))
				.withStatus(BucketLifecycleConfiguration.ENABLED.toString());

		BucketLifecycleConfiguration.Rule rule2 = new BucketLifecycleConfiguration.Rule()
				.withId("Archive and then delete rule")
				.withFilter(new LifecycleFilter(new LifecycleTagPredicate(new Tag("archive", "true"))))
				.addTransition(new Transition().withDays(90).withStorageClass(StorageClass.StandardInfrequentAccess))
				.addTransition(new Transition().withDays(365).withStorageClass(StorageClass.Glacier))
				.withExpirationInDays(3650).withStatus(BucketLifecycleConfiguration.ENABLED.toString());

		BucketLifecycleConfiguration configuration = new BucketLifecycleConfiguration()
				.withRules(Arrays.asList(rule1, rule2));
		SetBucketLifecycleConfigurationRequest lifeCycleRequest = new SetBucketLifecycleConfigurationRequest(bucket,
				configuration);

		// call to S3
		s3Client.setBucketLifecycleConfiguration(lifeCycleRequest);

		// get
		configuration = s3Client.getBucketLifecycleConfiguration(bucket);
		// modify
		configuration.setRules(Arrays.asList(rule2)); // only rule 2
		lifeCycleRequest.setLifecycleConfiguration(configuration);
		s3Client.setBucketLifecycleConfiguration(lifeCycleRequest);

		s3Client.deleteBucketLifecycleConfiguration(bucket);

		// look into following URL for conflicting/overlapping rules
		// https://docs.aws.amazon.com/AmazonS3/latest/dev/lifecycle-configuration-examples.html#lifecycle-config-conceptual-ex5

	}

	public void aclOperations(AmazonS3 s3Client) {
		// get, delete and add ACL
		AccessControlList acl = null;
		acl = s3Client.getBucketAcl(bucket); // get all

		acl.getGrantsAsList().clear();// delete, call setBucketAcl to remove all
										// old permission

		// different ways to add

		// email can only be registered aws account email id
		acl.grantPermission(new EmailAddressGrantee("email@email.com"),
				Permission.Read /*
								 * list permission at bucket level, on object
								 * level it is read object or it's meta data
								 */);

		acl.grantPermission(new CanonicalGrantee("Canonical ID "), Permission.Write);

		acl.getGrantsAsList().add(new Grant(GroupGrantee.AuthenticatedUsers, Permission.Read));

		acl.getGrantsAsList().add(new Grant(GroupGrantee.LogDelivery, Permission.Read));

		Owner owner = new Owner();
		owner.setId("owner cannonical id");
		owner.setDisplayName("Owner name");
		acl.setOwner(owner);

		// create and execute request
		SetBucketAclRequest request = new SetBucketAclRequest(bucket, acl);
		// update to S3
		s3Client.setBucketAcl(request);

	}

	/**
	 * list, set cors rules
	 * 
	 * @param s3Client
	 */
	public void corsOperations(AmazonS3 s3Client) {
		BucketCrossOriginConfiguration config;
		config = s3Client.getBucketCrossOriginConfiguration(bucket);

		// list
		if (config != null) {
			for (CORSRule rule : config.getRules()) {
				System.out.println("Allowed Headers " + rule.getAllowedHeaders());
				System.out.println("Allowed Methods " + rule.getAllowedMethods());
				System.out.println("Allowed Origin " + rule.getAllowedOrigins());
				System.out.println(
						"Max Age (Cache time before sending new preflight options request) " + rule.getMaxAgeSeconds());
			}

		}

		CORSRule corsRule1 = new CORSRule();
		CORSRule corsRule2 = new CORSRule();

		// atleast add these 2 for every cors rule
		corsRule1.setAllowedMethods(AllowedMethods.POST);
		corsRule1.setAllowedOrigins("amazon.com");

		corsRule2.setAllowedMethods(AllowedMethods.PUT);
		corsRule2.setAllowedOrigins("verysecuredomain.com");

		if (config == null) {
			config = new BucketCrossOriginConfiguration();
			config.setRules(new ArrayList<CORSRule>()); // empty list, otherwise
														// npe below
		}
		config.getRules().add(corsRule1);

		// another way to add with chaining
		// it overrides old rules, only rule 2 will be applied if run... this is
		// just to show way to do
		config.withRules(corsRule2);

		SetBucketCrossOriginConfigurationRequest request = new SetBucketCrossOriginConfigurationRequest(bucket, config);
		request.setCrossOriginConfiguration(config);

		// update
		s3Client.setBucketCrossOriginConfiguration(request);

		// refresh AWS console page, if it new rules does not show up :-)
	}

}
