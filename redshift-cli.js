#!/usr/bin/env node

var repl = require('repl');
var log4js = require('log4js');
var argv = require('optimist').argv;
var config = require('./lib/config');
var services = require('./lib/services');
var $u = require('util');

if (config.connectionString === undefined) {
	throw new Error('a connection string must be specified in config or in command line parameters');
	process.exit(1);
}

if (config.database === undefined) {
	throw new Error('a database must be specified in config or in command line parameters');
	process.exit(1);
}

if (config.log4js) {
	// very very shakey.....
	var pattern = config.log4js.appenders[0].appender.layout.pattern;

	config.log4js.appenders[0].appender.layout.pattern = pattern.replace(/_dbname_/, config.database);
	
	log4js.configure(config.log4js);
}

var logger = log4js.getLogger('redshift');

var replServer = repl.start({
	prompt: "redshift> "  
});

var context = replServer.context;

logger.info('please wait...');

services.init(config, context, function(err, results) {
	if (err) {
		logger.error(err);
		process.exit(1);
		return;
	}

	logger.info('ready\n\r');

	//require('fs').writeFileSync('zzz', $u.inspect(context))

	if (config.autorun) {
		replServer.commands['load'].action.call(replServer, config.autorun);
	}

	if (config.query) {		
		context.r.datastore.query(config.query, exitAfter);		
	}

	function exitAfter(err, results) {
		if (err) {
			logger.error(err);
			process.exit(1);
		} else {
			logger.info(results);
			process.exit(0);
		}
	}
});