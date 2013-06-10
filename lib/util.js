var logger = require('log4js').getLogger('redshift');

module.exports.createTemporarySecurityCredentials = function(sts, callback) {		
	sts.getSessionToken({ DurationSeconds: 86400 }, function(err, data) {
		if (err) {
			return logger.error(err);
		}

		logger.info('Temporary Security Credentials created successfully they will expire at: %s', data.Credentials.Expiration);
		
		logger.debug(data);

		callback(data.Credentials);
	});		
}
