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

	buildUpsert(tableName, insertColumns, insertValues, uniqueColumn, sets) {

		return `INSERT INTO ${tableName} (${insertColumns})` +
			` VALUES (${insertValues}) ON DUPLICATE KEY UPDATE ${sets}`;
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

	releaseConnection(source, connection, err) {

		if ((typeof source.getConnection) === 'function') {
			if (err)
				connection.destroy();
			else
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

	setSessionVariable(connection, varName, valueExpr, handler) {

		connection.query(`SET @${varName} = ${valueExpr}`, err => {

			if (err)
				handler.onError(err);
			else
				handler.onSuccess();
		});
	}

	getSessionVariable(connection, varName, type, handler) {

		connection.query(`SELECT @${varName}`, (err, result) => {

			if (err)
				return handler.onError(err);

			const valRaw = result[0]['@' + varName];

			if (valRaw === null)
				return handler.onSuccess();

			switch (type) {
			case 'number':
				handler.onSuccess(Number(valRaw));
				break;
			case 'boolean':
				handler.onSuccess(valRaw ? true : false);
				break;
			default:
				handler.onSuccess(valRaw);
			}
		});
	}

	selectIntoAnchorTable(
		connection, anchorTableName, topTableName, idColumnName, idExpr,
		statementStump, handler) {

		const trace = (handler.trace || function() {});

		const statusVarName = `@x2node.q.${topTableName}`;
		let sql;
		trace(sql = `SELECT ${statusVarName}`);
		connection.query(sql, (err, result) => {

			if (err)
				return handler.onError(err);

			if (result[0][statusVarName]) {
				trace(sql = `TRUNCATE TABLE ${anchorTableName}`);
				connection.query(sql, err => {

					if (err)
						return handler.onError(err);

					this._executeSelectIntoAnchorTable(
						connection, anchorTableName, idExpr, statementStump,
						handler, trace);
				});

			} else {

				trace(
					sql = `CREATE TEMPORARY TABLE ${anchorTableName}` +
						' (UNIQUE(id), ord INTEGER UNSIGNED NOT NULL UNIQUE)' +
						` AS SELECT ${idColumnName} AS id, 0 AS ord` +
						` FROM ${topTableName} WHERE ${idColumnName} IS NULL`
				);
				connection.query(sql, err => {

					if (err)
						return handler.onError(err);

					trace(sql = `SET ${statusVarName} = TRUE`);
					connection.query(sql, err => {

						if (err)
							return handler.onError(err);

						this._executeSelectIntoAnchorTable(
							connection, anchorTableName, idExpr, statementStump,
							handler, trace);
					});
				});
			}
		});
	}

	_executeSelectIntoAnchorTable(
		connection, anchorTableName, idExpr, statementStump, handler, trace) {

		let sql;
		if (this._options.mariaDB || this._isMariaDB(connection))
			trace(sql = (
				`INSERT INTO ${anchorTableName} (id, ord) ` +
				statementStump.replace(
					/\bSELECT\s+\{\*\}\s+FROM\b/i,
					`SELECT ${idExpr} AS id, ` +
					'(@x2node.ord := @x2node.ord + 1) AS ord FROM ' +
					'(SELECT @x2node.ord := 0) AS init,'
				)
			));
		else
			trace(sql = (
				`INSERT INTO ${anchorTableName} (id, ord) ` +
				'SELECT q.id AS id, ' +
				'(@x2node.ord := @x2node.ord + 1) AS ord FROM ' +
				'(SELECT @x2node.ord := 0) AS init, ' +
				'(' + statementStump.replace(
					/\bSELECT\s+\{\*\}\s+FROM\b/i,
					`SELECT ${idExpr} AS id FROM`) + ') AS q'
			));

		connection.query(sql, (err, result) => {

			if (err)
				return handler.onError(err);

			handler.onSuccess(result.affectedRows);
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

		connection.query(statement, (err, result) => {

			if (err)
				handler.onError(err);
			else
				handler.onSuccess(result.affectedRows);
		});
	}

	executeInsert(connection, statement, handler) {

		connection.query(statement, (err, result) => {

			if (err)
				handler.onError(err);
			else
				handler.onSuccess(result.insertId);
		});
	}

	createVersionTableIfNotExists(connection, tableName, itemNames, handler) {

		const trace = (handler.trace || function() {});

		let sql;
		trace(
			sql = `CREATE TABLE IF NOT EXISTS ${tableName} (` +
				'name VARCHAR(64) PRIMARY KEY, ' +
				'modified_on TIMESTAMP(3) DEFAULT 0, ' +
				'version INTEGER UNSIGNED NOT NULL)'
		);
		connection.query(sql, err => {
			if (err)
				return handler.onError(err);
			trace(sql = `LOCK TABLES ${tableName} WRITE`);
			connection.query(sql, err => {
				if (err)
					return handler.onError(err);
				trace(sql = `SELECT name FROM ${tableName}`);
				connection.query(sql, (err, result) => {
					if (err) {
						trace(sql = 'UNLOCK TABLES');
						return connection.query(
							sql, () => handler.onError(err));
					}
					const namesToInsert = new Set(itemNames);
					for (let i = 0, len = result.length; i < len; i++)
						namesToInsert.delete(result[i].name);
					if (namesToInsert.size > 0) {
						trace(
							sql = `INSERT INTO ${tableName}` +
								' (name, modified_on, version) VALUES ' +
								Array.from(namesToInsert).map(name => (
									'(' + this.stringLiteral(name) +
										', CURRENT_TIMESTAMP, 0)'
								)).join(', ')
						);
						connection.query(sql, err => {
							trace(sql = 'UNLOCK TABLES');
							connection.query(sql, err2 => {
								if (err || err2)
									return handler.onError(err || err2);
								handler.onSuccess();
							});
						});
					} else {
						trace(sql = 'UNLOCK TABLES');
						connection.query(sql, err => {
							if (err)
								return handler.onError(err);
							handler.onSuccess();
						});
					}
				});
			});
		});
	}

	updateVersionTable(
		connection, tableName, itemNames, modificationTimestamp, handler) {

		const filterExpr = 'name' + (
			itemNames.length === 1 ?
				' = ' + this.stringLiteral(itemNames[0]) :
				' IN (' + itemNames.map(v => this.stringLiteral(v)).join(', ') +
					')'
		);

		const trace = (handler.trace || function() {});
		let sql;
		trace(
			sql = `UPDATE ${tableName} SET ` +
				`modified_on = '${modificationTimestamp}', ` +
				`version = version + 1 WHERE ${filterExpr}`
		);
		connection.query(sql, (err, result) => {

			if (err)
				return handler.onError(err);

			if (result.affectedRows !== itemNames.length)
				return handler.onError(new Error(
					'Version rows are missing for some of the following' +
						' record types: ' + itemNames.join(', ')));

			return handler.onSuccess();
		});
	}

	_isMariaDB(connection) {

		const handshakePacket = (
			(
				connection._protocol &&
				connection._protocol._handshakeInitializationPacket
			) ||
			connection._handshakePacket
		);

		return (
			handshakePacket && /-MariaDB$/.test(handshakePacket.serverVersion)
		);
	}
}

module.exports = MySQLDBDriver;
