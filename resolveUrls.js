var async = require("async"),
	netUtils = require("./netUtils"),
	errCodes = require("./errorCodes");

module.exports = function(connection, table){

	function markDuplicate(id, callback){
		connection.query("UPDATE " + table + " SET `status` = ? WHERE `id` = " + id + ";", [errCodes['ER_DUP_ENTRY']], function(err, rows){
			if( err || rows.affectedRows === 0 ){ console.log("Error saving duplicate"); }
			callback();
		});
	}

	function updateRow(row, callback){

		var hostId = row.id;
		delete row.id;
		delete row.host;

		connection.query("UPDATE " + table + " SET ? WHERE `id` = " + hostId + ";", row, function(err, result){

			if( err ){
				if( err.code === 'ER_DUP_ENTRY' ){
					return markDuplicate(hostId, callback);
				}else{
					console.log("Error", err);
				}
			}

			callback();
		});
	}


	return function stream(callback){

		// Stream Table and ping
		connection.query("SELECT * FROM `" + table + "` WHERE `status` = '0' ORDER BY RAND() LIMIT 100;", function(err, rows){
			if( err ){ return console.log("Error streaming", err, this.sql); }

			if( rows.length === 0 ){
				return callback();
			}

			netUtils.processRows(rows, function(hosts){
				// console.log( hosts );

				async.each(
					hosts,
					updateRow,
					function(){
						stream(callback);
					}
				);
			});
		});
	};
};