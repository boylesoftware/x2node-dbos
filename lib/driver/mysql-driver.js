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

	constructor(options) {
		super(options);

		this._charset = ((options && options.databaseCharacterSet) || 'utf8');
	}

	supportsRowLocksWithAggregates() { return true; }

	safeLikePatternFromExpr(expr) {

		return `REPLACE(REPLACE(REPLACE(${expr}, '\\\\', '\\\\\\\\'),` +
			` '%', '\\%'), '_', '\\_')`;
	}

	stringSubstring(expr, from, len) {

		return 'SUBSTRING(' + expr +
			', ' + (
				(typeof from) === 'number' ?
					String(from + 1) : '(' + String(from) + ') + 1'
			) +
			(len !== undefined ? ', ' + String(len) : '') + ')';
	}

	nullableConcat() {

		return 'CONCAT(' + Array.from(arguments).join(', ') + ')';
	}

	castToString(expr) {

		return `CAST(${expr} AS CHAR)`;
	}

	patternMatch(expr, pattern, invert, caseSensitive) {

		return expr +
			' COLLATE ' + this._charset +
			(caseSensitive ? '_bin' : '_general_ci') +
			(invert ? ' NOT' : '') + ' LIKE ' + pattern;
	}

	regexpMatch(expr, regexp, invert, caseSensitive) {

		return expr +
			' COLLATE ' + this._charset +
			(caseSensitive ? '_bin' : '_general_ci') +
			(invert ? ' NOT' : '') + ' REGEXP ' + regexp;
	}

	/*datetimeToString(expr) {

		return 'DATE_FORMAT(' + expr + ', \'%Y-%m%dT%TZ\')';
	}*/

	makeRangedSelect(selectStmt, offset, limit) {

		return selectStmt + ' LIMIT ' +
			(offset > 0 ? String(offset) + ', ' : '') + limit;
	}

	makeSelectIntoTempTable(
		selectStmt, tempTableName, preStatements, postStatements) {

		preStatements.push(
			`CREATE TEMPORARY TABLE ${tempTableName} AS ${selectStmt}`);

		postStatements.push(
			`DROP TEMPORARY TABLE IF EXISTS ${tempTableName}`);
	}

	makeSelectWithLocks(selectStmt, exclusiveLockTables, sharedLockTables) {

		return selectStmt + (
			exclusiveLockTables && (exclusiveLockTables.length > 0) ?
				' FOR UPDATE' : (
					sharedLockTables && (sharedLockTables.length > 0) ?
						' LOCK IN SHARE MODE' : ''
				)
		);
	}

	buildLockTables() {

		throw new Error(
			'Internal X2 error: transaction scope table locks are' +
				' not supported by MySQL.');
	}

	buildDeleteWithJoins(
		fromTableName, fromTableAlias, refTables, filterExpr, filterExprParen) {

		const hasRefTables = (refTables && (refTables.length > 0));
		const hasFilter = filterExpr;

		return 'DELETE ' + fromTableAlias +
			' FROM ' + fromTableName + ' AS ' + fromTableAlias +
			(
				hasRefTables ?
					', ' + refTables.map(
						t => t.tableName + ' AS ' + t.tableAlias).join(', ') :
					''
			) +
			(
				hasRefTables || hasFilter ?
					' WHERE ' + (
						(
							hasRefTables ?
								refTables.map(
									t => t.joinCondition).join(' AND ') :
								''
						) + (
							hasFilter && hasRefTables ?
								' AND ' + (
									filterExprParen ?
										'(' + filterExpr + ')' : filterExpr
								) :
								''
						) + (
							hasFilter && !hasRefTables ? filterExpr : ''
						)
					) :
					''
			);
	}

	buildUpdateWithJoins(
		updateTableName, updateTableAlias, sets, refTables, filterExpr,
		filterExprParen) {

		const hasRefTables = (refTables && (refTables.length > 0));
		const hasFilter = filterExpr;

		return 'UPDATE ' + updateTableName + ' AS ' + updateTableAlias +
			(
				hasRefTables ?
					', ' + refTables.map(
						t => t.tableName + ' AS ' + t.tableAlias).join(', ') :
					''
			) +
			' SET ' + sets.map(
				s => updateTableAlias + '.' + s.columnName + ' = ' + s.value)
						.join(', ') +
			(
				hasRefTables || hasFilter ?
					' WHERE ' + (
						(
							hasRefTables ?
								refTables.map(
									t => t.joinCondition).join(' AND ') :
								''
						) + (
							hasFilter && hasRefTables ?
								' AND ' + (
									filterExprParen ?
										'(' + filterExpr + ')' : filterExpr
								) :
							''
						) + (
							hasFilter && !hasRefTables ? filterExpr : ''
						)
					) :
					''
			);
	}

	connect(source, handler) {

		if ((typeof source.getConnection) === 'function') {
			source.getConnection((err, connection) => {
				if (err)
					handler.onError(err);
				else
					handler.onSuccess(connection);
			});
		} else {
			source.connect(err => {
				if (err)
					handler.onError(err);
				else
					handler.onSuccess(source);
			});
		}
	}

	releaseConnection(source, connection) {

		if ((typeof source.getConnection) === 'function') {
			connection.release();
		} else {
			connection.end();
		}
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

	executeQuery(connection, statement, handler) {

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

	executeUpdate(connection, statement, handler) {

		const query = connection.query(statement);

		query[HAS_ERROR] = false;

		query.on('error', err => {
			if (query[HAS_ERROR])
				return;
			query[HAS_ERROR] = true;
			handler.onError(err);
		});

		let affectedRows;
		query.on('result', result => {
			affectedRows = result.affectedRows;
		});

		query.on('end', () => {
			if (!query[HAS_ERROR])
				handler.onSuccess(affectedRows);
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
