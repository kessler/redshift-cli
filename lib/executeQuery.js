var SimpleFileWriter = require('simple-file-writer');
var logger = require('log4js').getLogger('redshift');

var EMPTY_ARRAY = [];
var EMPTY_OBJECT = {};


/*
	execute an sql query against the given datastore with supplied params

	@param sql 		- sql to execute

	@param datastore- datastore object to use for the query

	@param params 	- a hash with the following: 
						projection: 	collect one field from all the rows
						filename: 		if specified save query results to a file instead of sending them to console
						values: 		an array containing values to be used in a parameterized query

	@param callback - will get call with query returns or errors
*/
function executeQuery(sql, datastore, params, callback) {
	if (typeof(params) === 'function') {
		callback = params;
		params = EMPTY_OBJECT;
	}

	if (params === undefined)
		params = EMPTY_OBJECT;

	if (params.values === undefined)
		params.values = EMPTY_ARRAY;

	var rowCount = 0;
	var result;

	var writer;
	if (params.filename) {
		var writer = new SimpleFileWriter(params.filename);
		onRow = function(row) {
			if (params.projection) {
				if (rowCount > 0)
					writer.write(',');

				writer.write(JSON.stringify(row[projection]));
			} else {
				if (rowCount > 0)
					writer.write('\n');

				writer.write(JSON.stringify(row));
			}

			rowCount++
		}
	} else {
		onRow = function(row) {
			if (params.projection) {
				if (rowCount > 0)
					result += ', ';

				result += row[projection];
			} else {
				logger.info(row);
			}

			rowCount++;
		}
	}

	var start = Date.now();

	logger.debug(sql);

	datastore.createQuery(sql, params.values, function(err, q) {
		logger.info('(you can keep doing other stuff in the meanwhile, just hit enter)');
		
		if (err) {
			if (callback === undefined)
				logger.error(err);
			else
				callback(err);
				
			return;
		}

		var hadErrorInQuery = false;

		q.on('error', function(err) {
			hadErrorInQuery = true;
			
			if (callback === undefined)			
				logger.error(err);
			else
				callback(err);
		});

		q.on('row', onRow);

		q.on('end', function(results) {
			var time = Date.now() - start;			
				
			logger.info('took %sms, %s rows returned', time, rowCount); 

			if (writer)
				writer.end();

			if (callback && !hadErrorInQuery) {				
				callback(null, result);

			} else if (result) {
				logger.info(result);			
			}			
		});
	});		
}

module.exports = executeQuery;