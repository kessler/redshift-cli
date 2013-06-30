var async = require('async');
var Datastore = require('db-stuff').Datastore;
var fs = require('fs');
var path = require('path');
var AWS = require('aws-sdk');
var $u = require('util');
var awsUtil = require('./awsUtil');

var unloadData = require('./unloadData');
var loadData = require('./loadData');
var searchUnloads = require('./searchUnloads');
var executeQuery = require('./executeQuery');

var logger = require('log4js').getLogger('redshift');
var inspect = require('eyes').inspector();

module.exports.init = function(config, context, done) {
	
	var operations = [ initHelperFunctions(config, context) ];
	
	if (config.accessKeyId && config.secretAccessKey && config.region)
		operations.push(initAwsSdk(config, context));
	else
		logger.warn('amazon sdk is not loaded since no configuration was supplied');

	operations.push(initDatastoreConnection(config, context));		
	
	async.waterfall(operations, done);
};

/*
	load a query from a file and use r.query() to execute it

	@param filename 	- a text file containing an sql query

	@param params 		- params object, see query.js for reference
*/
function queryFromFile(filename, datastore, params, callback) {

	logger.info('loading query from file %s', filename);
	fs.readFile(filename, 'utf8', function (err, data) {	
		if (err) {
			logger.error(err);
			return;
		}

		logger.info('loaded: %s\nexecuting...', err);

		executeQuery(data, datastore, params, callback);
	});
}

/*
	list active queries in the cluster
*/
function activeQueries(datastore) {
	executeQuery("select pid, query from stv_recents where status='Running'", datastore);
}

/*
	cancel a running query
*/
function cancelQuery(datastore, pid) {
	executeQuery('cancel ' + pid, datastore);
}

/*
	describe the objects in the cluster (or database ???)
*/
function describe(datastore, databaseName, callback) {	
	//TODO tried to convert this to parameterized query but then I get all the fields back as ? instead of text...
	executeQuery("select datname, nspname, relname, sum(rows) as rows from pg_class, pg_namespace, pg_database, stv_tbl_perm where pg_namespace.oid = relnamespace and pg_class.oid = stv_tbl_perm.id and pg_database.oid = stv_tbl_perm.db_id and datname = '"+databaseName+"' group by datname, nspname, relname order by datname, nspname, relname;", datastore, callback);
}

/*
	initialize datastore connection and functions

*/
function initDatastoreConnection(config, context) {
	return function(callback) {
		logger.info('connecting to datastore...');
	
		if (config.connectionString !== undefined && config.connectionString.substr(-1) !== '/')
			config.connectionString += '/';

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

	r.query = function(sql, params, callback) {			
		if (!sql) {
			logger.info('usage: r.query(sql, params, callback)');
			return;
		}

		executeQuery(sql, datastore, params, callback);
	};

	r.activeQueries = function () {
		activeQueries(datastore);
	};

	r.cancelQuery = function (pid) {
		if (!pid) {
			logger.info('usage: r.cancelQuery(pid)');
			return;
		}
		cancelQuery(datastore, pid);
	};

	r.queryFromFile = function(filename, params, callback) {
		if (!filename) {
			logger.info('usage: r.queryFromFile(filename, params, callback)');
			return;
		}

		queryFromFile(datastore, filename, params, callback);
	};

	r.describe = function(databaseName, callback) {		
		if (typeof(databaseName) === ' function') {
			callback = databaseName;
			databaseName = config.database;
		} else {			
			databaseName = databaseName || config.database;			
		}

		describe(datastore, databaseName, callback);
	};

	// do not expose if required amazon functionality is missing
	if (context.s3enabled) {

		/*
			see unloadData.js for details
		*/
		r.unloadData = function(name, query, gzip, delimiter, bucketName) {
			if (!name) {		
				logger.info('usage: r.unloadData(name, query /*optional*/, gzip /*optional*/, delimiter /*optional*/, bucketName /*optional*/)');
				return;			
			}

			if (bucketName === undefined && config.defaultS3BucketName === undefined) {
				throw new Error('must specify defaultS3BucketName in config or bucketName in call to unloadData');
			}

			bucketName = bucketName || config.defaultS3BucketName;
			
			if (gzip === undefined)
				gzip = config.gzip;

			if (delimiter === undefined)
				delimiter = config.delimiter;

			var credentials;

			if (typeof (config.overrideS3SecurityCredentials) === 'object') {

				logger.warn('using credentials from config (this is not recommended)');
				unloadData(datastore, config.overrideS3SecurityCredentials, config.database, name, query, gzip, delimiter, bucketName);
				
			} else if (typeof (context.sts) === 'object') {

				logger.info('creating Temporary Security Credentials...');
				awsUtil.createTemporarySecurityCredentials(context.sts, function(err, tempCredentials) {
					unloadData(datastore, tempCredentials, config.database, name, query, gzip, delimiter, bucketName);	
				});

			} else {
				logger.error('S3 was configured, but neither Amazon STS or adhoc credentials were supplied, cannot continue...');
			}
		};

		/*

		*/
		r.loadData = function(table, path, bucket, gzip, delimiter, callback) {
			if (path === undefined || table === undefined) {		
				logger.info('usage: r.loadData(table, path, bucket /*optional*/, gzip /*optional*/, delimiter /*optional*/, callback /*optional*/)');
				return;			
			}

			if (gzip === undefined)
				gzip = config.gzip;

			if (delimiter === undefined)
				delimiter = config.delimiter;

			if (bucket === undefined) {

				if (config.defaultS3BucketName === undefined) 
					throw new Error('missing bucket name, either define in config or supply as parameter');

				bucket = config.defaultS3BucketName;
			}

			loadData(datastore, config, table, path, bucket, gzip, delimiter, callback);
		};

		/*
			see searchUnloads.js for details

			bucketName is optional (depending on config)
		*/
		r.searchUnloads = function(bucketName, callback) {
			
			if (config.defaultS3BucketName === undefined && typeof (bucketName) !== 'string') {
				throw new Error('config does not contain defaultS3BucketName and no other bucket name was specified')
			}

			if (typeof (bucketName) === 'function') {
				callback = bucketName;
				bucketName = config.defaultS3BucketName;
			}

			searchUnloads(bucketName, callback);	
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
		context.inspectCallback = function(err) {
			if (err)
				logger.error(inspect(err));
			else	
				logger.info(inspect(Array.prototype.slice.call(arguments, 1)));
		};

		callback(null);
	}
}