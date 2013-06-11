var logger = require('log4js').getLogger('redshift');
var AWS = require('aws-sdk');
var $l = require('lodash');

function appendToTree(tree, path, index) {
	var key = path[index];

	if (index === path.length - 2) {				
		if (tree[key] === undefined) tree[key] = [];		
		tree[key].push(path[index + 1]);
		return;
	} 

	if (tree[key] === undefined) {
		tree[key] = {};
	}

	appendToTree(tree[key], path, ++index);
}

function searchUnloads(bucketName, callback) {

	if (callback === undefined)
		throw new Error('doesn\'t make sense to call this function without a callback');

	logger.info('searching unloads in %s', bucketName);

	var s3params = { 
		Bucket: bucketName.replace('s3://', '')
	}
	
	var s3 = new AWS.S3({
		params: s3params
	});

	s3.listObjects(function(err, data) {

		if (err) {
			return logger.error(err);
		}

		if (data.Contents === undefined) {
			callback(null, []);
		} else if (data.Contents.length === 0) {
			callback(null, data.Contents);
		} else {
			
			var results = {}
			var foundSomething = false;

			for (var i = 0; i < data.Contents.length; i++) {
				var file = data.Contents[i];
				
				var key = file.Key.split('/');

				if (key.length < 4) continue;

				if (key[3].indexOf('_part_') > -1) {					
					foundSomething = true;
					appendToTree(results, key, 0);
				}
			}

			callback(null, foundSomething, results);
		}
	});
}



module.exports = searchUnloads;