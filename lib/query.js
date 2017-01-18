'use strict';

const common = require('x2node-common');
const RSParser = require('x2node-rsparser');


/**
 * The query.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class Query {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using
	 * [QueryFactory]{@link module:x2node-queries~QueryFactory}.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} topRecordTypeName Name of the top record type to fetch.
	 */
	constructor(dbDriver, recordTypes, topRecordTypeName) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._topRecordTypeName = topRecordTypeName;

		//...

		this._withSuperaggregates = false;

		this._queries = this._assembleQueryBuilders(
			this._buildQueryTree(), false
		).map(queryBuilder => queryBuilder.toSQL());
	}

	/**
	 * Build query tree.
	 * TODO: write jsdoc tags
	 *
	 * @private
	 */
	_buildQueryTree() {

		//...
		return  {
			required: true,
			table: 'scrapreasons',
			tableAlias: 'za',
			keyColumn: 'id',
			keyProps: [
				{
					expr: 'za.id',
					markup: 'id'
				}
			],
			ownProps: [
				{
					expr: 'za.name',
					markup: 'name'
				}
			],
			children: []
		};
	}

	/**
	 * Assemble query builders from the query tree.
	 * TODO: write jsdoc tags
	 *
	 * @private
	 */
	_assembleQueryBuilders(queryTreeNode, forceOuter) {

		// build FROM clause element
		const from = queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias + (
			queryTreeNode.parent ?
				' ON ' + queryTreeNode.tableAlias + '.' +
				queryTreeNode.keyColumn + ' = ' +
				queryTreeNode.parent.tableAlias + '.' +
				queryTreeNode.keyColumnInParent :
				''
		);

		// build SELECT list of the key properties
		const keys = queryTreeNode.keyProps.map(
			prop => prop.expr + ' AS ' + this._dbDriver.safeLabel(prop.markup));

		// build SELECT list of the rest of the node's properties
		const data = queryTreeNode.ownProps.map(
			prop => prop.expr + ' AS ' + this._dbDriver.safeLabel(prop.markup));

		// build ORDER BY list of the key properties
		const keysOrder = queryTreeNode.keyProps.map(prop => prop.expr);

		// TODO: build and add custom order by and where
		//...

		// create query builder for node
		const queryBuilder = {
			collection: queryTreeNode.many,
			outer: (forceOuter || !queryTreeNode.required),
			from: from,
			select: keys.concat(data),
			orderBy: [],

			toSQL() {
				return 'SELECT ' + this.select.join(', ') +
					' FROM ' + this.from + (
						this.orderBy.length > 0 ?
							' ORDER BY ' + this.orderBy.join(', ') : ''
					);
			}
		};

		// add children and return the result
		return queryTreeNode.children.reduce((res, childNode) => {

			this._assembleQueryBuilders(
				childNode, queryBuilder.outer
			).forEach((childQueryBuilder, i) => {

				if ((i === 0) && (
					!queryBuilder.collection || !childQueryBuilder.collection
				)) {

					if (childQueryBuilder.collection) {
						queryBuilder.collection = true;
						queryBuilder.orderBy.push.apply(
							queryBuilder.orderBy, keysOrder);
					}

					queryBuilder.from += (
						childQueryBuilder.outer ?
							' LEFT OUTER JOIN ' : ' INNER JOIN '
					) + childQueryBuilder.from;

					queryBuilder.select.push.apply(
						queryBuilder.select, childQueryBuilder.select);

					queryBuilder.orderBy.push.apply(
						queryBuilder.orderBy, childQueryBuilder.orderBy);

				} else {

					childQueryBuilder.from = from + (
						childQueryBuilder.outer ?
							' LEFT OUTER JOIN ' : ' INNER JOIN '
					) + childQueryBuilder.from;

					childQueryBuilder.select =
						keys.concat(childQueryBuilder.select);

					childQueryBuilder.orderBy =
						keysOrder.concat(childQueryBuilder.orderBy);

					res.push(childQueryBuilder);
				}
			});

			return res;

		}, [ queryBuilder ]);
	}

	/**
	 * Execute the query.
	 *
	 * @param {*} connection The database connection compatible with the database
	 * driver.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {external:Promise.<Object>} The result promise. The result object
	 * has <code>records</code> and optionally <code>referredRecords</code>
	 * properties plus any requested super-aggregates.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(connection, filterParams) {

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// queue up pre-statements
		if (this._preStatements)
			this._preStatements.forEach(stmt => {
				resPromise = resPromise.then(
					() => new Promise((resolve, reject) => {
						this._dbDriver.execute(
							connection,
							this._replaceParams(stmt, filterParams),
							{
								onSuccess() {
									resolve();
								},
								onError(err) {
									reject(err);
								}
							}
						);
					}),
					err => Promise.reject(err)
				);
			});

		// queue up main queries
		this._queries.forEach((query, index) => {
			resPromise = resPromise.then(
				rootParser => new Promise((resolve, reject) => {
					let parser = new RSParser(
						this._recordTypes, this._topRecordTypeName);
					this._dbDriver.execute(
						connection,
						this._replaceParams(query, filterParams),
						{
							onHeader(fieldNames) {
								parser.init(fieldNames);
							},
							onRow(row) {
								parser.feedRow(row);
							},
							onSuccess() {
								resolve(
									rootParser ? rootParser.merge(parser) :
										parser);
							},
							onError(err) {
								reject(err);
							}
						}
					);
				}),
				err => Promise.reject(err)
			);
		});

		// transform the parser into the result object
		resPromise = resPromise.then(
			rootParser => {
				const res = new Object();
				if (this._withSuperaggregates) {
					const superRec = rootParser.records[0];
					Object.keys(superRec).forEach(propName => {
						res[propName] = superRec[propName];
					});
				} else {
					res.records = rootParser.records;
				}
				const refRecs = rootParser.referredRecords;
				if (Object.keys(refRecs).length > 0)
					res.referredRecords = refRecs;
				return res;
			},
			err => Promise.reject(err)
		);

		// queue up post-statements
		if (this._postStatements)
			this._postStatements.forEach(stmt => {
				resPromise = resPromise.then(
					res => new Promise((resolve, reject) => {
						this._dbDriver.execute(
							connection,
							this._replaceParams(stmt, filterParams),
							{
								onSuccess() {
									resolve(res);
								},
								onError(err) {
									reject(err);
								}
							}
						);
					}),
					err => Promise.reject(err)
				);
			});

		// return the result promise chain
		return resPromise;
	}

	/**
	 * Replace parameter placeholders in the specified SQL statement with the
	 * corresponding values.
	 *
	 * @private
	 * @param {string} stmtText SQL statement text with parameter placeholders.
	 * Each placeholder has format "${name}" where "name" is the parameter name.
	 * @param {Object.<string,*>} params Parameter values by parameter name.
	 * Array values are expanded into comma-separated lists of element values.
	 * @returns {string} SQL statement with parameters inserted.
	 * @throws {module:x2node-common.X2UsageError} If provided parameters object
	 * is invalid (missing parameter, NaN value).
	 */
	_replaceParams(stmtText, params) {

		let res = '';

		const re = new RegExp('(\'(?!\'))|(\')|\\?\\{([^}]+)\\}', 'g');
		let m, inLiteral = false, lastMatchIndex = 0;
		while ((m = re.exec(stmtText)) !== null) {
			res += stmtText.substring(lastMatchIndex, m.index);
			lastMatchIndex = re.lastIndex;
			const s = m[0];
			if (inLiteral) {
				res += s;
				if (m[1]) {
					inLiteral = false;
				} else if (m[2]) {
					re.lastIndex++;
				}
			} else {
				if (s === '\'') {
					res += s;
					inLiteral = true;
				} else {
					const paramName = m[3];
					const val = params[paramName];
					if (Array.isArray(val)) {
						val.forEach((valEl, index) => {
							if (index > 0)
								res += ', ';
							res += this._toSQLLiteral(valEl, paramName);
						});
					} else {
						res += this._toSQLLiteral(val, paramName);
					}
				}
			}
		}
		res += stmtText.substring(lastMatchIndex);

		return res;
	}

	/**
	 * Convert specified value to a SQL literal.
	 *
	 * @private
	 * @param {*} val The value. If function, the function is called with no
	 * arguments and the result is used as the value.
	 * @returns {string} The SQL literal.
	 * @throws {module:x2node-common.X2UsageError} If provided value is invalid,
	 * such as <code>undefined</code> or <code>NaN</code>.
	 */
	_toSQLLiteral(val, paramName) {

		if ((typeof val) === 'function')
			val = val.call(null);

		if (val === undefined)
			throw new common.X2UsageError(
				'Missing query parameter ' + paramName + '.');
		if (Number.isNaN(val))
			throw new common.X2UsageError(
				'Query parameter ' + paramName +
					' is NaN, which is not allowed.');

		if (val === null)
			return 'NULL';

		switch (typeof val) {
		case 'boolean':
			return this._dbDriver.booleanLiteral(val);
		case 'number':
			return String(val);
		case 'string':
			return '\'' + val.replace('\'', '\'\'') + '\'';
		default:
			throw new common.X2UsageError(
				'Query parameter ' + paramName + ' has unsupported value type ' +
					(typeof val) + '.');
		}
	}
};

module.exports = Query;
