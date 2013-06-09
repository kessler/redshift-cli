
var async = require('async');
var Datastore = require('db-stuff').Datastore;
var logger = require('log4js').getLogger('redshift');
var fs = require('fs');
var path = require('path');
var SimpleFileWriter = require('simple-file-writer');

/*
	initialize datastore connection and functions

*/
function initDatastoreConnection(context, config) {
	return function(callback) {
		logger.info('initializing datastore');
		
		Datastore.create(config, function(err, datastore) {
			if (err) {
				return callback(err);
			} else {
				context.r = context.redshift = {};
				context.r.datastore = datastore;

				initDatastoreFunctions(datastore, context.r, callback);								
			}
		});
	};
}

/*
	initialize datastore functions
*/
function initDatastoreFunctions(datastore, r, callback) {
	r.query = function(sql, options) {	
		options = options || {};
		var writer;
		if (options.filename) {
			var writer = new SimpleFileWriter(options.filename);
			onRow = function(row) {
				if (options.projection) {
					writer.write(JSON.stringify(row[projection]) + '\n');
				} else {
					writer.write(JSON.stringify(row) + '\n');
				}
			}
		} else {
			onRow = function(row) {
				if (options.projection) {
					logger.info(row[projection]);
				} else {
					logger.info(row);
				}
			}
		}

		var start = Date.now();

		datastore.createQuery(sql, function(err, q) {
			if (err) {
				return logger.error(err);
			}

			q.on('error', function(err) {
				logger.error(err);
			});

			q.on('row', onRow);

			q.on('end', function() {
				var time = Date.now() - start;			
				logger.info('took %sms', time); 
				if (writer)
					writer.end();
			});
		});		
	};

	r.activeQueries = function () {
		r.query("select pid, query from stv_recents where status='Running'");
	};

	r.cancelQuery = function (pid) {
		r.query('cancel ' + pid);
	};

	r.queryFromFile = function(filename, options) {
		logger.info('loading query from file %s', filename);
		fs.readFile(filename, 'utf8', function (err, data) {	
			if (err) {
				logger.error(err);
				return;
			}

			logger.info('loaded: %s\nexecuting...', err);

			r.query(data, options);
		});
	};

	r.describe = function () {
		r.query("select datname, nspname, relname, sum(rows) as rows from pg_class, pg_namespace, pg_database, stv_tbl_perm where pg_namespace.oid = relnamespace and pg_class.oid = stv_tbl_perm.id and pg_database.oid = stv_tbl_perm.db_id and datname ='rtb' group by datname, nspname, relname order by datname, nspname, relname;");
	};

	callback(null);
}

module.exports.init = function(config, context, done) {
	
	//var operations = [];
	//operations.push(initDatastoreConnection(context, config));		
	//async.waterfall(operations, done);

	initDatastoreConnection(context, config)(done);
};