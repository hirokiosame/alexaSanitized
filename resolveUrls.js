var async = require("async"),
	netUtils = require("./netUtils");

module.exports = function(connection, table){

	function markDuplicate(id, callback){
		addStatus("Duplicate", function(err, statusCode){
			if( err ){
				console.log("Error fetching status code", err);
				callback(err);
				return;
			}

			connection.query("UPDATE " + table + " SET `status` = ? WHERE `id` = " + id + ";", [statusCode], function(err, rows){
				if( err || rows.affectedRows === 0 ){ console.log("Error saving duplicate", this.sql); }
				callback();
			});
		});
	}

	var statuses = {};

	function getStatusId(status, callback){

		// Check Cache
		if( statuses[status] ){ callback(null, statuses[status]); return; }

		// Check MySQL
		connection.query(
			"SELECT statusId FROM " + table + "_statuses WHERE statusMessage = ?;",
			[status],
			function(err, result){
				if( err ){
					console.log("Error selecting statusId from table", err, this.sql);
					callback(err);
					return;
				}

				// Selected
				if( result.length === 0 ){ return callback(null, null); }

				// Validate result
				if( typeof result[0] !== "object" || !result[0].hasOwnProperty("statusId") || typeof result[0]['statusId'] !== "number" ){
					console.log("Error selecting statusId: invalid result format", result, this.sql);
					getStatusId(status, callback);
					return;
				}else{
					callback(null, (statuses[status] = result[0]['statusId']));
				}
			}
		);
	}

	function addStatus(status, callback){

		getStatusId(status, function(err, statusId){
			if( err ){
				callback(err);
				return;
			}

			if( typeof statusId === "number" ){
				callback(null, statusId);
				return;
			}

			connection.query(
				"INSERT IGNORE INTO " + table + "_statuses (statusMessage) VALUES (?);",
				[status],
				function(err, result){
					if( err ){
						console.log("Error inserting status", err, this.sql);
						callback(err);
						return;
					}

					if( typeof result !== "object" || !result.hasOwnProperty("affectedRows") || typeof result.affectedRows !== "number" ){
						console.log("Error inserting status, invalid result format", result, this.sql);
						return addStatus(status, callback);
					}

					// Inserted
					if( result.affectedRows === 1 ){
						callback(null, (statuses[status] = result.insertId)); return;
					}

					// Simultaneously added?
					else if( statuses[status] ){
						callback(null, statuses[status]); return;
					}

					else{
						console.log("Error inserting status", arguments, this.sql, statuses, statusId);
						addStatus(status, callback);
						return;
					}
				}
			);
		});
	}

	function updateRow(row, callback){

		var hostId = row.id;
		delete row.id;
		delete row.host;


		addStatus(row.status, function(err, statusCode){
			if( err ){
				console.log("Error fetching status code", err);
				callback(err);
				return;
			}

			row.status = statusCode;

			connection.query("UPDATE " + table + " SET ? WHERE `id` = " + hostId + ";", row, function(err, result){

				if( err ){
					if( err.code === 'ER_DUP_ENTRY' ){
						return markDuplicate(hostId, callback);
					}else{
						console.log("Error Duplicate", err, this.sql);
					}
				}

				callback();
			});
		});
	}


	return function stream(callback){

		// Stream Table and ping
		connection.query("SELECT * FROM `" + table + "` WHERE `status` IS NULL ORDER BY RAND() LIMIT 100;", function(err, rows){
			if( err ){ return console.log("Error streaming", err, this.sql); }

			if( rows.length === 0 ){
				return callback();
			}

			netUtils.processRows(rows, function(hosts){
				async.each(
					hosts,
					updateRow,
					function(err){
						if( err ){
							console.log("Error occurred", err);
						}

						// To avoid recursion...
						process.nextTick(function(){
							stream(callback);	
						});
					}
				);
			});
		});
	};
};