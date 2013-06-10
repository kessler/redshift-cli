var logger = require('log4js').getLogger('redshift');

module.exports = function(config, context, params, callback) {

	if (params.bucketName === undefined && config.defaultS3BucketName === undefined) {
		throw new Error('neither params.bucketName or config.defaultS3BucketName are specified');
	}

	if (params.bucketName === undefined)
		params.bucketName = config.defaultS3BucketName;

	params.bucketName += '/' + config.database + '/';

	var s3 = ~~~~~~context.s3;

	s3.listObject({ Bucket: params.bucketName }, function(err, data) {

		if (err) {
			return logger.error(err);
		}

		logger.info(data.Contents);

	});

}