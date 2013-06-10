var SimpleFileWriter = require('simple-file-writer');
var logger = require('log4js').getLogger('redshift');

var empty = [];


/*
	execute an sql query against the given datastore with supplied options

	@param sql 		- sql to execute

	@param options 	- a hash with the following: 
						projection: 	collect one field from all the rows
						filename: 		if specified save query results to a file instead of sending them to console
						values: 		an array containing values to be used in a parameterized query

	@param datastore- datastore object to use for the query
*/
function query(sql, options, datastore) {
	options = options || {};

	if (!options.values)
		options.values = empty;

	var rowCount = 0;
	var result;

	var writer;
	if (options.filename) {
		var writer = new SimpleFileWriter(options.filename);
		onRow = function(row) {
			if (options.projection) {
				if (rowCount++ > 0)
					writer.write(',');

				writer.write(JSON.stringify(row[projection]));
			} else {
				if (rowCount++ > 0)
					writer.write('\n');

				writer.write(JSON.stringify(row));
			}
		}
	} else {
		onRow = function(row) {
			if (options.projection) {
				if (rowCount++ > 0)
					result += ', ';

				result += row[projection];
			} else {
				logger.info(row);
			}
		}
	}

	var start = Date.now();

	datastore.createQuery(sql, options.values, function(err, q) {
		if (err) {
			return logger.error(err);
		}

		q.on('error', function(err) {
			logger.error(err);
		});

		q.on('row', onRow);

		q.on('end', function(results) {
			if (result)
				logger.info(result);

			var time = Date.now() - start;			
			logger.info('took %sms', time); 

			if (writer)
				writer.end();

			if (options.callback)
				options.callback();
		});
	});		
}

module.exports = query;