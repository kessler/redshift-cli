var logger = require('log4js').getLogger('redshift');
var $u = require('util');
var AWS = require('aws-sdk');
var util = require('./util');

/*
	Unload data from redshift to S3.

	@param config 	- this is usually the main configuration of the cli but can be anything the contains the keys: "overrideS3SecurityCredentials", "database" and optionally "defaultS3BucketName". 
						If "overrideS3SecurityCredentials" is present we will not generate Temporary Security Credentials for the operation. "overrideS3SecurityCredentials" is of the form:
						{ SecretAccessKey: 'something', AccessKeyId: 'something' } (case sensitive of course)

	@param context	- main context of the cli, checked and accessed for various dependencies needed for this operation

	@param params   - specific parameters needed for the unload operation:
						name:  the name of the operation, if query key is absent from params, then name is treated as a table name and I will use "select * from [name]".
								data will be unloaded to s3://[bucketName]/[config.database]/[name]

						bucketName: override defaultS3BucketName behavior, unload will be written to this bucket instead. Must be an absolute s3 bucket address

						query: use this query instead of automatic select all

						
*/
function unloadData(config, context, params) {
	if (params === undefined)		
		throw new Error('missing params');

	if (params.bucketName === undefined)
		throw new Error('must specify a bucket name');

	if (params.name === undefined) 
		throw new Error('must specify a name');

	
	if (params.query === undefined) 
		params.query = 'select * from ' + params.name;
	
	var now = new Date().toISOString();

	var sts = context.sts;

	var s3params = { 
		Bucket: params.bucketName.replace('s3://', ''),
		Key: config.database + '/' + params.name + '/' + now + '/'
	}
	
	var s3 = new AWS.S3({
		params: s3params
	});

	var r = context.r;

	logger.info('Initiating unload operation.\nbucket: s3://%s/%s\nquery: %s', params.bucketName, s3params.Key, params.query);

	if (config.overrideS3SecurityCredentials) {

		logger.warn('using credentials from config (this is not recommended)');
		executeUnloadQuery(config.overrideS3SecurityCredentials);

	} else if (sts) {

		logger.info('creating Temporary Security Credentials...');
		util.createTemporarySecurityCredentials(sts, executeUnloadQuery);

	} else {
		logger.error('S3 was configured, but neither Amazon STS or adhoc credentials were supplied, cannot continue...');
	}
	
	function executeUnloadQuery(credentials) {		
		var unloadQuery = 'unload (\'' + params.query + '\') ';
		unloadQuery += 'to \'s3://' + params.bucketName + '/' + s3params.Key + '\' ';
		unloadQuery += 'credentials \'aws_access_key_id=' + credentials.AccessKeyId + ';aws_secret_access_key=' + credentials.SecretAccessKey;
		
		if (config.overrideS3SecurityCredentials) 
			unloadQuery += '\'';
		else
			unloadQuery += ';token=' + credentials.SessionToken + '\'';

		logger.debug(unloadQuery);

		function saveDataSql() {
			var s3DataSqlParams = { 
				Key: s3params.Key + 'data.sql', 
				Body: params.query
			};

			logger.debug(s3DataSqlParams)

			s3.putObject(s3DataSqlParams, function(err, data) {
				if (err)
					return logger.error(err);

				logger.debug(data);
			});
		}

		saveDataSql();

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