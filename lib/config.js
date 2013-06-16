/*************************************************************************************************
	DO NOT MODIFY THIS FILE UNLESS YOU REALLY REALLY REALLY REALLY KNOW WHAT YOU ARE DOING!!!  /k
*************************************************************************************************/

var rc = require('rc')
var argv = require('optimist').argv //ALWAYS USE OPTIMIST FOR COMMAND LINE OPTIONS.

var logger = require('log4js').getLogger('config');

if (argv.config)
	logger.info('config: %s', argv.config);

var defaults = 	{	
	log4js: {
		appenders: [
	        {
	            type: 'logLevelFilter',
	            level: 'INFO',
	            appender: {
	                type: 'console',
	                layout: {
	                    type: 'pattern',
	                    pattern: '%[%n[%r _dbname_]%] %m'
	                }                
	            }
	        }
	    ]
	},
	implementation: 'PostgresDatastore',
	workDirectory: process.cwd()
};

module.exports = rc('redshift-cli', defaults);
