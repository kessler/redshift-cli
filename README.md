redshift command line interface
===============================

###start the command line
```
	node redshift-cli.js --configPath=[some.config.json]
```
all config options are overridable from environment or from a command line argument with the same name

-------------

### required config for minimal database access
```
{
	"connectionString": "[connection string including user and password (obtainable via redshift aws console) - WITHOUT THE DATABASE NAME]",
	"database": "[name of the database]"
}
```

-------------

### required config for unload and load functionality
```
{
	"connectionString": "[connection string including user and password (obtainable via redshift aws console) - WITHOUT THE DATABASE NAME]",
	"database": "[name of the database]",
	"accessKeyId": "[Access key with permissions to create temporary security token and access the s3 bucket where we store all the unloads]", 
	"secretAccessKey": "[Secret access key of the above]",	
	"region": "[amazon region to be used (e.g us-east-1)]",
	"defaultS3BucketName": "[bucket where we store unloads and loads]",	
	"sslEnabled": true	
}
```

-------------

### How to do stuff:
Lets say we have table Foo in database meow:
```
id | name | bar
---+------+-----
1  | moo  | zzz
---+------+-----
2  | oom  | eek
---+------+-----
3  | dog  | rrr
```
etc...

-------------

###### Simple query:
```
redshift> r.query('select * from foo')
```
will print:
```
{
	"id": 1,
	"name": "moo",
	"bar": "zzz"
}

//and so on ...
```

-------------

###### Query with projection:
```
redshift> r.query('select * from foo', { projection: 'bar' });
```
will print:
```
	zzz, eek, rrr
```

-------------

###### Stream query results to file (row by row):
```
redshift> r.query('select * from foo', { filename: 'meow.json' });
```
will write meow.json:
```
{ "id": 1, "name": "moo", "bar": "zzz" }\n
{ "id": 2, "name": "oom", "bar": "eek" }\n
{ "id": 3, "name": "dog", "bar": "rrr" }\n
```
or with projection
```
redshift> r.query('select * from foo', { filename: 'meow.json', projection: 'id' });
```
will write meow.json:
```
1,2,3
```

-------------

###### Load query from a file:
```
redshift> r.queryFromFile('zzz.sql')
```
will print ... etc...

Another example:
```
redshift> r.queryFromFile('zzz.sql', { filename: 'result.json', projection: 'name' })
```

-------------

###### List active queries:
```
redshift> r.activeQueries();
```
will print a list of active queries with their pids

-------------

###### cancel an active query:
```
redshift> r.cancelQuery(1232);
```
will cancel query with pid 1232

-------------

###### Unload data from redshift to s3
```
/*
	given config.json contains: 
	{ 
		"database": "meow",
		"defaultS3BucketName": "myunloads"
	}
*/

redshift> r.unloadData('foo')
```
will unload to s3://myunloads/meow/foo/[date string in iso format gmt 0 timezone]/0001_part... etc etc... 
using Temporary Security Credentials generate from sts using getSessionToken()

it will also create a data.sql in the same location containing the sql used to select the data for unloading
```
redshift> r.unloadData('foo', 'select * from foo join pie on foo.id=pie.id')
```

-------------

###### Autorun
```
/*
	start.js:

	r.query('select * from foo');
	console.log('bye');
*/

/*
	config.json: 
	{
		autorun: 'start.js'
	}
*/

node redshift-cli.js --configPath=....
```

Will start the cli execute start.js in the context of the CLI

-------------

###### optional config (add to above hashes)
{
	"overrideS3SecurityCredentials": {
		"AccessKeyId": "[]",
		"SecretAccessKey": "[]"
	}
}

-------------

*TODO: add to npm (need to add db-stuff too)*