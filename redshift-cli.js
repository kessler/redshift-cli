var repl = require('repl');
var logger = require('log4js').getLogger('redshift');
var argv = require('optimist').argv;
var config = require('./lib/config');
var services = require('./lib/services');
var $u = require('util');

if (config.get('connectionString') === undefined) {
	throw new Error('a connection string must be specified in config or in command line parameters');
	process.exit(1);
}

var context = repl.start({
	prompt: "redshift> "  
}).context;

logger.info('please wait...');

services.init(config.store, context, function(err, results) {
	logger.info('ready\n\r');
});