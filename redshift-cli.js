var repl = require('repl');
var log4js = require('log4js');
var argv = require('optimist').argv;
var config = require('./lib/config');
var services = require('./lib/services');
var $u = require('util');

if (config.store.connectionString === undefined) {
	throw new Error('a connection string must be specified in config or in command line parameters');
	process.exit(1);
}

if (config.store.database === undefined) {
	throw new Error('a database must be specified in config or in command line parameters');
	process.exit(1);
}

if (config.store.log4js) {
	// very very shakey.....
	var pattern = config.store.log4js.appenders[0].appender.layout.pattern;

	config.store.log4js.appenders[0].appender.layout.pattern = pattern.replace(/_dbname_/, config.store.database);
	
	log4js.configure(config.store.log4js);
}

var logger = log4js.getLogger('redshift');

var replServer = repl.start({
	prompt: "redshift> "  
});

logger.info('please wait...');

services.init(config.store, replServer.context, function(err, results) {
	logger.info('ready\n\r');

	if (config.store.autorun) {
		replServer.commands['.load'].action.call(replServer, config.store.autorun);		
	}
});

