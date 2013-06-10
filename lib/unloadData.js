var logger = require('log4js').getLogger('redshift');
var $u = require('util');
var AWS = require('aws-sdk');

/*
	Unload data from redshift to S3.

	@param config 	- this is usually the main configuration of the cli but can be anything the contains the keys: "overrideS3SecurityCredentials", "database" and optionally "defaultS3BucketName". 
						If "overrideS3SecurityCredentials" is present we will not generate Temporary Security Credentials for the operation. "overrideS3SecurityCredentials" is of the form:
						{ SecretAccessKey: 'something', AccessKeyId: 'something' } (case sensitive of course)

	@param context	- main context of the cli, checked and accessed for various dependencies needed for this operation

	@param params   - specific parameters needed for the unload operation:
						name:  the name of the operation, if query key is absent from params, then name is treated as a table name and I will use "select * from [name]".
								additionally if bucketName is absent and config contains the key "defaultS3BucketName" then the target bucket will be s3://[defaultS3BucketName]/[config.database]/[name]

						query: use this query instead of automatic select all

						bucketName: override defaultS3BucketName behavior, unload will be written to this bucket instead. Must be an absolute s3 bucket address
*/
function unloadData(config, context, params) {
	if (params === undefined)
		throw new Error('missing params');

	if (params.name === undefined) {
		if (params.query === undefined)
			throw new Error('when not specifying a name, query param must be used');

		if (params.bucketName === undefined)
			throw new Error('when not specifying a name, bucketName param must be used');
	}  

	
	if (params.query === undefined) {
		params.query = 'select * from ' + params.name;
	}

	if (params.bucketName === undefined) {
		if (config.defaultS3BucketName === undefined) 
			throw new Error('neither params.bucketName or config.defaultS3BucketName were specified, cannot continue...');

		params.bucketName = 's3://' + config.defaultS3BucketName;		
	}

	var now = new Date().toISOString();

	var unloadKey = config.database + '/' + params.name + '/' + now + '/';
	
	var sts = context.sts;
	
	var s3 = new AWS.S3({
		params: { 
			Bucket: params.bucketName			
		}
	});

	var r = context.r;

	logger.info('Initiating unload operation.\nbucket:%s\nquery:%s', unloadKey, params.query);

	if (config.overrideS3SecurityCredentials) {

		logger.warn('using credentials from config (this is not recommended)');
		executeUnloadQuery(config.overrideS3SecurityCredentials);

	} else if (sts) {

		logger.info('creating Temporary Security Credentials...');
		createTemporarySecurityCredentials(executeUnloadQuery);

	} else {
		logger.error('S3 was configured, but neither Amazon STS or adhoc credentials were supplied, cannot continue...');
	}

	function createTemporarySecurityCredentials(callback) {		
		sts.getSessionToken({ DurationSeconds: 86400 }, function(err, data) {
			if (err) {
				return logger.error(err);
			}

			logger.info('Temporary Security Credentials created successfully they will expire at: %s', data.Credentials.Expiration);
			
			logger.debug(data);

			callback(data.Credentials);
		});		
	}
	
	function executeUnloadQuery(credentials) {		
		var unloadQuery = 'unload (\'' + params.query + '\') ';
		unloadQuery += 'to \'' + params.bucketName + '/' + unloadKey + '\' ';
		unloadQuery += 'credentials \'aws_access_key_id=' + credentials.AccessKeyId + ';aws_secret_access_key=' + credentials.SecretAccessKey;
		
		if (config.overrideS3SecurityCredentials) 
			unloadQuery += '\'';
		else
			unloadQuery += ';token=' + credentials.SessionToken + '\'';

		logger.debug(unloadQuery);

		function saveDataSql() {
			var s3DataSqlParams = { 
				Key: unloadKey + 'data.sql', 
				Body: params.query
			};

			console.log(s3DataSqlParams)

			s3.putObject(s3DataSqlParams, function(err, data) {
				if (err)
					return logger.error(err);

				logger.debug(data);
			});
		}

		//saveDataSql();

		logger.info('unload operation in progress...');

		var options = {
			callback: function() {				
				saveDataSql();
				logger.info('unload operation done...');
			}
		}

		r.query(unloadQuery, options);
	}
}

module.exports = unloadData;