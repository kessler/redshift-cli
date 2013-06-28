var logger = require('log4js').getLogger('redshift');
var $u = require('util');
var AWS = require('aws-sdk');
var executeQuery = require('./executeQuery');

/*
	Unload data from redshift to S3.

	@param datastore 	- an instance of Datastore to use

	@param credentials 	- a set of amazon credentials to use

	@param database		- the name of the database we're unloading data from 
	
	@param name 		- the name of the operation, if query param is absent, then name is treated as a table name and I will use "select * from [name]".
						additionally data will be unloaded to s3://[bucketName]/[config.database]/[name]/data/

	@param gzip			- instruct redshift to unload and gzip

	@param delimiter	- field delimiter

	@param query 		- use this query instead of automatic select all

	@param bucketName 	- Where the unload operation will write its results - Must be an absolute s3 bucket address
						
*/
function unloadData(datastore, credentials, database, name, gzip, delimiter, query, bucketName) {
	
	if (name === undefined) 
		throw new Error('must specify a name');

	if (bucketName === undefined)
		throw new Error('must specify a bucket name');
	
	if (query === undefined) 
		query = 'select * from ' + name;
	
	var now = new Date().toISOString();

	var unloadPath = database + '/' + name + '/' + now + '/'

	var s3 = new AWS.S3({
		params: { 
			Bucket: bucketName.replace('s3://', '')	
		}
	});

	logger.info('Initiating unload operation.\nbucket: s3://%s/%s\nquery: %s', bucketName, unloadPath, query);
	
	executeUnloadQuery();

	function saveDataSql() {
		var s3DataSqlParams = { 
			Key: unloadPath + 'data_query.sql', 
			Body: query
		};

		logger.debug(s3DataSqlParams)

		s3.putObject(s3DataSqlParams, function(err, data) {
			if (err)
				logger.error(err);
			else
				logger.debug(data);
		});
	}

	function executeUnloadQuery() {		
		var unloadQuery = 'unload (\'' + query + '\') ';
		unloadQuery += 'to \'s3://' + bucketName + '/' + unloadPath + 'data/\' ';
		unloadQuery += 'credentials \'aws_access_key_id=' + credentials.AccessKeyId + ';aws_secret_access_key=' + credentials.SecretAccessKey;
		
		if (credentials.SessionToken === undefined) 
			unloadQuery += '\'';
		else
			unloadQuery += ';token=' + credentials.SessionToken + '\'';

		unloadQuery += ' delimiter \'' + delimiter + '\'';

		if (gzip) {
			unloadQuery += ' gzip';
		}

		saveDataSql();

		logger.info('unload %s.%s operation in progress...', database, name);

		executeQuery(unloadQuery, datastore, function(err) {				
			if (err)
				logger.error(err);
			else
				logger.info('unload %s.%s operation done...', database, name);
		});
	}
}

module.exports = unloadData;