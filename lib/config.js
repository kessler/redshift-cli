/*************************************************************************************************
	DO NOT MODIFY THIS FILE UNLESS YOU REALLY REALLY REALLY REALLY KNOW WHAT YOU ARE DOING!!!  /k
*************************************************************************************************/

var cc = require('config-chain')
var opts = require('optimist').argv //ALWAYS USE OPTIMIST FOR COMMAND LINE OPTIONS.
var path = require('path');

var configPath = opts.config || process.env.redshift_cli_config;

var logger = require('log4js').getLogger('config');

if (configPath)
	logger.info('config path: %s', configPath);

var conf = cc(

	//OVERRIDE SETTINGS WITH COMMAND LINE OPTS
	opts,

	//ENV VARS IF PREFIXED WITH 'myApp_'
	cc.env('redshift_cli'), //myApp_foo = 'like this'

    configPath,

	//PUT DEFAULTS LAST
	{	
		implementation: 'PostgresDatastore'
	}
);

module.exports = conf;
