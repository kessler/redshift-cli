var logger = require('log4js').getLogger('redshift');

function loadData(config, context, bucket, table) {

	var r = context.r;

	var query = 'copy ' + table + ' from \'' + bucket + '\'';
	query += 'aws_access_key_id=<your-access-key-id>;aws_secret_access_key=<your-secret-access-key>'
	delimiter '|';

}

module.exports = loadData;