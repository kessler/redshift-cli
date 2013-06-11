var logger = require('log4js').getLogger('redshift');

function loadData(config, context, bucket, table) {

	var r = context.r;

	var query = 'copy ' + table + ' from \'' + bucket + '\' ';
	query += '\'aws_access_key_id=' + config.accessKeyId + ';aws_secret_access_key=' + config.secretAccessKey + '\' ';
	query += 'delimiter \'|\'';

	logger.info('about to load data to redshift, using this query:\n%s', query);

	r.query(query, function() {

	});

}

module.exports = loadData;