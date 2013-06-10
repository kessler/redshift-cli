
var async = require('async');
var Datastore = require('db-stuff').Datastore;
var logger = require('log4js').getLogger('redshift');
var fs = require('fs');
var path = require('path');

var AWS = require('aws-sdk');
var $u = require('util');

var unloadData = require('./unloadData');
var listUnloads = require('./listUnloads');
var query = require('./query');


/*
	load a query from a file and use r.query() to execute it

	@param filename 	- a text file containing an sql query

	@param options 		- an options hash, see query() above for reference
*/
function queryFromFile(filename, options, r) {

	logger.info('loading query from file %s', filename);
	fs.readFile(filename, 'utf8', function (err, data) {	
		if (err) {
			logger.error(err);
			return;
		}

		logger.info('loaded: %s\nexecuting...', err);

		r.query(data, options);
	});
}

/*
	list active queries in the cluster
*/
function activeQueries(r) {
	r.query("select pid, query from stv_recents where status='Running'");
}

/*
	cancel a running query
*/
function cancelQuery(r, pid) {
	r.query('cancel ' + pid);
}

/*
	describe the objects in the cluster (or database ???)
*/
function describe(r, databaseName) {
	//TODO tried to convert this to parameterized query but then I get all the fields back as ? instead of text...
	r.query("select datname, nspname, relname, sum(rows) as rows from pg_class, pg_namespace, pg_database, stv_tbl_perm where pg_namespace.oid = relnamespace and pg_class.oid = stv_tbl_perm.id and pg_database.oid = stv_tbl_perm.db_id and datname = '"+databaseName+"' group by datname, nspname, relname order by datname, nspname, relname;");
}

/*
	initialize datastore connection and functions

*/
function initDatastoreConnection(config, context) {
	return function(callback) {
		logger.info('initializing datastore');
	
		var dbconfig =  {
			connectionString: config.connectionString + config.database,
			implementation: config.implementation
		};

		Datastore.create(dbconfig, function(err, datastore) {
			if (err) {
				return callback(err);
			} else {
				context.r = context.redshift = {};
				context.r.datastore = datastore;

				initDatastoreFunctions(datastore, config, context, callback);								
			}
		});
	};
}

/*
	initialize datastore functions
*/
function initDatastoreFunctions(datastore, config, context, callback) {
	var r = context.r;

	r.query = function(sql, options) {	
		query(sql, options, datastore, context);
	};

	r.activeQueries = function () {
		activeQueries(context.r);
	};

	r.cancelQuery = function (pid) {
		cancelQuery(context.r, pid);
	};

	r.queryFromFile = function(filename, options) {
		queryFromFile(filename, options, context.r);
	};

	r.describe = function (databaseName) {
		describe(context.r, databaseName);
	};

	// do not expose if require amazon functionality is missing
	if (context.s3enabled) {

		/*
			see unloadData.js for details

			query and bucketName are basically optional
		*/
		r.unloadData = function(name, query, bucketName) {
			var params = {
				name: name,
				query: query,
				bucketName: bucketName || config.defaultS3BucketName
			};

			unloadData(config, context, params);
		};

		r.loadData = function(bucket) {

		};

		/*
			see listUnloads.js for details

			bucketName is optional (depending on config)
		*/
		r.listUnloads = function(bucketName) {
			var params = {				
				bucketName: bucketName
			};

			listUnloads(config, context, params);	
		};

	} else {
		logger.warn('Aws S3 was not configured, load and unload functionality will be disabled');				
	}

	callback(null);
}

function initAwsSdk(config, context) {
	return function(callback) {				
		AWS.config.update(config);	

		context.s3enabled = true;
		context.sts = new AWS.STS();

		callback(null);
	}
}

function initHelperFunctions(config, context) {
	return function(callback) {
		context.simpleCallback = function() {
			logger.info(arguments);
		};

		callback(null);
	}
}

module.exports.init = function(config, context, done) {
	
	var operations = [ initHelperFunctions(config, context) ];
	
	if (config.accessKeyId && config.secretAccessKey && config.region)
		operations.push(initAwsSdk(config, context));
	else
		logger.warn('amazon sdk is not loaded since no configuration was supplied');

	operations.push(initDatastoreConnection(config, context));		
	
	async.waterfall(operations, done);
};