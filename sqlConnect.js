module.exports = function(host, user, password, database, table, callback){

	var mysql = require("mysql");

	var connection = mysql.createConnection({
		host: host,
		user: user,
		password: password,
		database: database,
		multipleStatements: true
	});


	// Create table
	function createTable(table, callback){
		console.log("Creating table");
		connection.query('CREATE TABLE IF NOT EXISTS `' + table + '`(' +
			'id INT NOT NULL AUTO_INCREMENT,' +
			'PRIMARY KEY (id),' +
			'host VARCHAR(100) NOT NULL,' +
			'UNIQUE (`host`),' +
			'resolvedTo VARCHAR(100) DEFAULT NULL,' +
			'UNIQUE (`resolvedTo`),' +
			'status INT DEFAULT NULL,' +
			'FOREIGN KEY(status) REFERENCES ' + table + '_statuses(statusId)' +
		');', function(err, rows){
			if( err ){
				console.log(err, this.sql);
				return callback(err);
			}

			console.log("Successfully created table.");
			callback(null, rows);
		});	
	}


	// Create table
	function createStatusTable(table, callback){
		console.log("Creating error table");
		connection.query('CREATE TABLE IF NOT EXISTS `' + table + '_statuses`(' +
			'statusId INT NOT NULL AUTO_INCREMENT,' +
			'PRIMARY KEY (statusId),' +
			'statusMessage VARCHAR(100) NOT NULL,' +
			'UNIQUE (`statusMessage`)' +
		');', function(err, rows){
			if( err ){
				console.log(err, this.sql);
				return callback(err);
			}

			console.log("Successfully error table.");
			callback(null, rows);
		});	
	}


	// Connect
	connection.connect(function(err){

		if( err ){ return callback(err); }

		console.log("Successfully connected to MySQL.");


		// Check if table exists and get stats if does
		connection.query(
			"SELECT * FROM information_schema.tables WHERE table_schema = '" + database + "' AND table_name = '" + table + "' LIMIT 1;",
			// "DROP TABLE IF EXISTS `" + table + "`;",
			function(err, rows){
				if( err ){ throw err; }

				// Create
				if( rows.length === 0 ){
					console.log("Table doesn't exist...");

					createStatusTable(table, function(err, fields){
						if(err){ throw err; }

						createTable(table, function(err, fields){
							if(err){ throw err; }

							callback(null, connection);
						});
					});
				}

				// Already Exists
				else{
					console.log("Table exists");
					callback(null, connection);
				}

				/*
				// Delete
				else{
					console.log("Table already exists.");

					// connection.query("DROP TABLE `" + table + "`;", function(err, row){
					// 	if( err ){ throw err; }
						// console.log("drop table", row);
						createTable(table, function(err, fields){
							if(err){ return console.log(err); }
							console.log(fields);
							callback(null, connection);
						});
					// });
				}*/
			}
		);
	});


	// Catches ctrl+c event to exit properly
	process
	.on('SIGINT', process.exit)

	// Cleanup before exit
	.on('exit', function(){

		console.log("Exit");

		// Kill MySql connection
		connection.end();
	});
};