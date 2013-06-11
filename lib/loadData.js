var logger = require('log4js').getLogger('redshift');
var executeQuery = require('./executeQuery');

function naiveCallback(table) {
	return function(err) {

		if (err === undefined)
			logger.info('load operation to %s completed', table);
		else
			logger.error(err);	
	}
}

/*
	@param datastore 		- datastore to run the load query against
	@param table			- the table to load data into
	@param credentials		- amazon credentials
	@param bucket			- 
	@param path				- path to raw data files inside the bucket
	@param callback 		- optional callback 
*/
function loadData(datastore, table, credentials, bucket, path, callback) {

	function loadDataImpl() {

		var accessKeyId = credentials.accessKeyId || credentials.AccessKeyId; // here we go, upper lower case crap
		var secretAccessKey = credentials.secretAccessKey || credentials.SecretAccessKey;

		var fullPath = 's3://' + bucket + '/' + path;

		var query = 'copy ' + table + ' from \'' + fullPath + '\' ';

		query += '\'aws_access_key_id=' + accessKeyId + ';aws_secret_access_key=' + secretAccessKey + '\' ';
		query += 'delimiter \'|\'';

		logger.info('about to load data to redshift, using this query:\n%s', query);

		callback = callback || naiveCallback; 
		
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