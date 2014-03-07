redshift command line interface
===============================

Useful for doing manual stuff on redshift and as a tool for cron jobs and such

###Install
```
	npm install -g redshift-cli
```

### start the CLI
```
	redshift-cli --config=[some.config.json]

	//or (see quick query below)

	redshift-cli --config=[some.config.json] --query="select * from lala_land"

	//or (see auto run below)

	redshift-cli --config=[some.config.json] --autorun=/home/me/start.js
```
-------------

##### Autorun
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

redshift-cli --config=....

// or

redshift-cli --config=[some.config.json] --autorun=/home/me/start.js

```

Will start the cli and execute start.js in the context of the CLI

-------------

##### Quick query
```
	redshift-cli --query="select * from lala_land"
```
will execute this query and exit the process afterwards, exit code will indicate the successfulness of the query

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
more on [optional config keys](#optional-config-keys)

redshift-cli config is using the awesome [rc lib](https://github.com/dominictarr/rc)

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

##### Simple query:
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

##### Query with projection:
```
redshift> r.query('select * from foo', { projection: 'bar' });
```
will print:
```
	zzz, eek, rrr
```

-------------

##### Stream query results to file (row by row):
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

##### describe tables
```
redshift> r.describe();

//or

redshift> r.describe('dbname');

//

redshift> r.describe(function(err, results) { // do something with it });
```

-------------

##### show create DDL script for tables
```

redshift> r.showCreate('schema.tablename');

```

-------------

##### Load query from a file:
```
redshift> r.queryFromFile('zzz.sql')
```
will use content of zzz.sql as query and print the result

Another example:
```
redshift> r.queryFromFile('zzz.sql', { filename: 'result.json', projection: 'name' })
```

will use content of zzz.sql as query and save the result to 'result.json'

-------------

##### List active queries:
```
redshift> r.activeQueries();
```
will print a list of active queries with their pids

-------------

##### cancel an active query:
```
redshift> r.cancelQuery(1232);
```
will cancel query with pid 1232

-------------

##### Unload data from redshift to s3
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
will unload to s3://myunloads/meow/foo/[date string in iso format gmt 0 timezone]/data/0001_part... etc etc... 
using Temporary Security Credentials generate from sts using getSessionToken()

it will also create s3://myunloads/meow/foo/[date string in iso format gmt 0 timezone]/data.sql containing the sql used to select the data for unloading
```
redshift> r.unloadData('foo', 'select * from foo order by id', true, ',')
```
order by id when unloading, enable gzip and use ',' as delimiter

-------------
##### search for previous unloads
```
redshift> r.searchUnloads(inspectCallback)
```
will output:
```
redshift> 
    {
        meow: {
            foo: {
                2013-06-11T06:21:01.135Z: [
                    '0000_part_00',
                    '0001_part_00',
                    '0002_part_00',
                    '0003_part_00',
                    '0004_part_00',
                    '0005_part_00',
                    '0006_part_00',
                    '0007_part_00'
                ],
                2013-06-11T06:20:53.936Z: [
                    '0000_part_00',
                    '0001_part_00',
                    '0002_part_00',
                    '0003_part_00',
                    '0004_part_00',
                    '0005_part_00',
                    '0006_part_00',
                    '0007_part_00'
                ]
            }
        }
    }
```

-------------

##### load back to redshift
```
redshift> r.loadData('foo', 'meow/foo/2013-06-11T06:21:01.135Z')
```
will load back data into foo.

-------------

##### optional config keys
use fixed credentials for unload/load operations (instead of temporary ones)
```
{
	"overrideS3SecurityCredentials": {
		"AccessKeyId": "[]",
		"SecretAccessKey": "[]"
	}
}
```
use gzip or replace default delimiter
```
{
	"delimiter": ",",
	"gzip": true

}
```

#####TODO
-Add data verification for load operations - probably using data saved during unload (select count(*) and such)
-Add auto table generation - need something like pg_dump
-Auto 'select starttime, filename, err_reason from stl_load_errors order by starttime desc limit 100' with filename like '' when load fails
