var services = require('../lib/services');
var Datastore = require('db-stuff').Datastore;
var config = { implementation: 'DevelopmentDatastore' };
var assert = require('assert');

describe('services for cli - ', function () {
	
	it('connects to the datastore', function (done) {
		
		services.init(config, context, function(err) {		
			if (err) assert.fail(err);
			else assert.ok(context.r.datastore instanceof Datastore.DevelopmentDatastore);
			done();
		});
	});

	it('populates context with various functions', function (done) {
		var context = {};
		services.init(config, context, function(err) {
		
			if (err) return assert.fail(err);
			assert.strictEqual('function', typeof context.r.activeQueries);
			assert.strictEqual('function', typeof context.r.query);
			assert.strictEqual('function', typeof context.r.queryFromFile);

			done();
		});
	});
});