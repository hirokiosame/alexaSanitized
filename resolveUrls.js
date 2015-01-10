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

	function addStatus(status, callback){
		if( statuses[status] ){
			return callback(null, statuses[status]);
		}
		connection.query(
			"INSERT IGNORE INTO " + table + "_statuses (statusMessage) VALUES (?);" +
			"SELECT statusId FROM " + table + "_statuses WHERE statusMessage = ?;",
			[status, status],
			function(err, result){
				if( err ){
					console.log("Error inserting status", err, this.sql);
					callback(err);
					return;
				}
				
				if( result[1] && result[1][0] && result[1][0]['statusId'] ){
					callback(null, (statuses[status] = result[1][0]['statusId']));	
				}else{
					console.log("ERROR! can't find statusId", JSON.stringify(result, 0, 3));
				}
			}
		);
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