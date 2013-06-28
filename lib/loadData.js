var logger = require('log4js').getLogger('redshift');
var executeQuery = require('./executeQuery');

function naiveCallback(table) {
	return function(err) {
		
		if (err === null)
			logger.info('load operation to %s completed', table);
		else
			logger.error(err);	
	}
}

/*
	@param datastore 		- datastore to run the load query against
	@param credentials		- amazon credentials
	@param table			- the table to load data into	
	@param path				- path to raw data files inside the bucket
	@param bucket			- 
	@param gzip				- expect gzipped files
	@param delimiter		- field delimiter
	@param callback 		- optional callback 
*/
function loadData(datastore, credentials, table, path, bucket, gzip, delimiter, callback) {

	function loadDataImpl() {

		var accessKeyId = credentials.accessKeyId || credentials.AccessKeyId; // here we go, upper lower case crap
		var secretAccessKey = credentials.secretAccessKey || credentials.SecretAccessKey;

		if (path[0] !== '/')
			path = '/' + path;

		var fullPath = 's3://' + bucket + path;

		var query = 'copy ' + table + ' from \'' + fullPath + '\' ';

		query += 'credentials \'aws_access_key_id=' + accessKeyId + ';aws_secret_access_key=' + secretAccessKey + '\'';
		query += ' delimiter \'' + delimiter + '\'';

		if (gzip)
			query += ' gzip';

		logger.info('loading data to %s from %s', table, fullPath);

		callback = callback || naiveCallback(table); 
		
		executeQuery(query, datastore, callback);
	}

	executeQuery('select * from ' + table + ' limit 1', datastore, function(err, data) {
		
		if (err === null) {
			
			loadDataImpl();

		} else {

			logger.error('could not proceed with load operation due to: %s', err);		
		}

	});
}

module.exports = loadData;