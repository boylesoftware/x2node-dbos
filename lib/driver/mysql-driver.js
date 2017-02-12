'use strict';

const BasicDBDriver = require('./basic-driver.js');


/**
 * Symbol used to indicate that there is an error on the query object.
 *
 * @private
 */
const HAS_ERROR = Symbol();

/**
 * MySQL database driver.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 * @extends {module:x2node-queries~BasicDBDriver}
 * @implements {module:x2node-queries.DBDriver}
 */
class MySQLDBDriver extends BasicDBDriver {

	nullableConcat(str, expr) {

		return 'CONCAT(' + this.stringLiteral(str) + ', ' + expr + ')';
	}

	castToString(expr) {

		return 'CAST(' + expr + ' AS CHAR)';
	}

	/*datetimeToString(expr) {

		return 'DATE_FORMAT(' + expr + ', \'%Y-%m%dT%TZ\')';
	}*/

	stringSubstring(expr, from, len) {

		return 'SUBSTRING(' + expr + ', ' + (from + 1) +
			(Number.isNaN(len) ? '' : ', ' + len) + ')';
	}

	makeRangedSelect(selectStmt, offset, limit) {

		return selectStmt + ' LIMIT ' +
			(offset > 0 ? String(offset) + ', ' : '') + limit;
	}

	execute(connection, statement, handler) {

		const query = connection.query(statement);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess();
		});

		if (handler.onHeader)
			query.on('fields', fields => {
				if (query[HAS_ERROR])
					return;
				try {
					handler.onHeader(fields.map(field => field.name));
				} catch (err) {
					query[HAS_ERROR] = true;
					handler.onError(err);
				}
			});

		if (handler.onRow)
			query.on('result', row => {
				if (query[HAS_ERROR])
					return;
				try {
					handler.onRow(row);
				} catch (err) {
					query[HAS_ERROR] = true;
					handler.onError(err);
				}
			});
	}
}

module.exports = MySQLDBDriver;
