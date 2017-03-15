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
 * @memberof module:x2node-dbos
 * @inner
 * @extends {module:x2node-dbos~BasicDBDriver}
 * @implements {module:x2node-dbos.DBDriver}
 */
class MySQLDBDriver extends BasicDBDriver {

	safeLikePatternFromExpr(expr) {

		return 'REPLACE(REPLACE(REPLACE(' + expr +
			', \'\\\\\', \'\\\\\\\\\'), \'%\', \'\\%\'), \'_\', \'\\_\')';
	}

	nullableConcat() {

		return 'CONCAT(' + Array.from(arguments).join(', ') + ')';
	}

	patternMatch(expr, pattern, invert, caseSensitive) {

		return expr +
			' COLLATE ' + (caseSensitive ? 'utf8_bin' : 'utf8_general_ci') +
			(invert ? ' NOT' : '') + ' LIKE ' + pattern;
	}

	regexpMatch(expr, regexp, invert, caseSensitive) {

		return expr +
			' COLLATE ' + (caseSensitive ? 'utf8_bin' : 'utf8_general_ci') +
			(invert ? ' NOT' : '') + ' REGEXP ' + regexp;
	}

	castToString(expr) {

		return 'CAST(' + expr + ' AS CHAR)';
	}

	/*datetimeToString(expr) {

		return 'DATE_FORMAT(' + expr + ', \'%Y-%m%dT%TZ\')';
	}*/

	stringSubstring(expr, from, len) {

		return 'SUBSTRING(' + expr +
			', ' + (
				(typeof from) === 'number' ?
					String(from + 1) : '(' + String(from) + ') + 1'
			) +
			(len !== undefined ? ', ' + String(len) : '') + ')';
	}

	makeRangedSelect(selectStmt, offset, limit) {

		return selectStmt + ' LIMIT ' +
			(offset > 0 ? String(offset) + ', ' : '') + limit;
	}

	makeSelectIntoTempTable(
		selectStmt, tempTableName, preStatements, postStatements) {

		preStatements.push(
			'CREATE TEMPORARY TABLE ' + tempTableName + ' AS ' +
				selectStmt);

		postStatements.push(
			'DROP TEMPORARY TABLE IF EXISTS ' + tempTableName);
	}

	startTransaction(connection, handler) {

		connection.query('START TRANSACTION', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	rollbackTransaction(connection, handler) {

		connection.query('ROLLBACK', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	commitTransaction(connection, handler) {

		connection.query('COMMIT', err => {
			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
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

	executeInsert(connection, statement, handler) {

		const query = connection.query(statement);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		let id;
		query.on('result', result => {
			id = result.insertId;
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess(id);
		});
	}
}

module.exports = MySQLDBDriver;
