'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');
const rangeBuilder = require('./range-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');
const Transaction = require('./transaction.js');


/**
 * Filter parameters handler used by a query.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class FilterParamsHandler {

	/**
	 * Create new parameters handler for a query.
	 */
	constructor() {

		this._nextParamRef = 0;

		this._params = new Map();
	}

	addParam(paramName, valueFunc) {

		const paramRef = String(this._nextParamRef++);

		this._params.set(paramRef, {
			name: paramName,
			valueFunc: valueFunc
		});

		return '?{' + paramRef + '}';
	}

	paramValueToSql(dbDriver, paramValue, valueFunc, paramName) {

		function invalid() {
			new common.X2UsageError(
				'Invalid ' + (paramName ? '"' + paramName + '"' : '') +
					' filter test parameter value ' + String(paramValue) +
					'.');
		}

		let preSql;
		if (valueFunc) {
			if ((paramValue === undefined) || (paramValue === null) || (
				((typeof paramValue) === 'number') &&
					!Number.isFinite(paramValue)) ||
				Array.isArray(paramValue))
				throw invalid();
			preSql = valueFunc(paramValue);
		} else {
			preSql = paramValue;
		}

		const sql = dbDriver.sql(preSql);
		if ((sql === null) || (sql === 'NULL'))
			throw invalid();

		return sql;
	}

	paramSql(dbDriver, filterParams, paramRef) {

		const ref = this._params.get(paramRef);
		const filterParam = filterParams[ref.name];
		if (filterParam === undefined)
			throw new common.X2UsageError(
				'Missing filter parameter "' + ref.name + '".');

		const process = v => this.paramValueToSql(
			dbDriver, v, ref.valueFunc, ref.name);
		return (
			Array.isArray(filterParam) ?
				filterParam.map(v => process(v)).join(', ') :
				process(filterParam)
		);
	}
}

/**
 * Fetch (select) query.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class FetchQuery {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using
	 * [QueryFactory]{@link module:x2node-queries~QueryFactory}.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~RecordTypeDescriptor} superTypeDesc
	 * Descriptor of the fetched record type's super-type.
	 * @param {string[]} [selectedPropPatterns] Selected record property
	 * patterns, or nothing if records should not be fetched.
	 * @param {string[]} [selectedSuperProps] Selected super-property names, or
	 * nothing if no super-properties need to be fetched.
	 * @param {Array[]} [filterSpec] Optional specification of the filter to
	 * apply to the selected records.
	 * @param {string[]} [orderSpec] Optional order specification to apply to the
	 * selected records.
	 * @param {number[]} [rangeSpec] Optional range specification to apply to the
	 * selected records.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the underlying record types library is not
	 * suitable for it.
	 */
	constructor(
		dbDriver, recordTypes, superTypeDesc, selectedPropPatterns,
		selectedSuperProps, filterSpec, orderSpecs, rangeSpec) {

		// the debug log
		this._log = common.getDebugLogger('X2_QUERY');

		// save the basics
		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
		this._recordTypeName = superTypeDesc.recordsRecordTypeName;
		this._superTypeName = superTypeDesc.name;

		// create base value expressions context
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypes.getRecordTypeDesc(this._recordTypeName) ]);

		// get records property descriptor
		const recordsPropDesc = superTypeDesc.getPropertyDesc('records');

		// build top query filter and the tree of properties used in it
		const usedPropPaths = new Set();
		const filter = (filterSpec && filterBuilder.buildFilter(
			recordTypes,
			(colPropPath, propPaths) => propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'where', baseValueExprCtx,
				colPropPath, propPaths, {
					noWildcards: true,
					noAggregates: true,
					ignoreScopedOrders: true,
					noScopedFilters: true,
					includeScopeProp: true
				}
			)[0],
			recordsPropDesc, baseValueExprCtx, [ ':and', filterSpec ],
			usedPropPaths));
		const filterPropsTree = (
			filter && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'where', baseValueExprCtx, '',
				usedPropPaths, {
					noWildcards: true,
					noAggregates: true,
					ingnoreScopedOrders: true,
					noScopedFilters: true
				})[0]);

		// build top query order and the tree of properties used in it
		usedPropPaths.clear();
		const order = (orderSpecs && orderBuilder.buildOrder(
			baseValueExprCtx, orderSpecs, usedPropPaths));
		const orderPropsTree = (
			order && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'orderBy', baseValueExprCtx, '',
				usedPropPaths, {
					noWildcards: true,
					noAggregates: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);

		// build the range object
		const range = (rangeSpec && rangeBuilder.buildRange(rangeSpec));

		// filter parameters handler
		this._paramsHandler = new FilterParamsHandler();

		// build super properties queries
		this._superPropsQueries = new Array();
		if (selectedSuperProps && selectedSuperProps.length > 0) {
			// TODO: ...
		}

		// build records queries
		this._preStatements = new Array();
		this._queries = new Array();
		this._postStatements = new Array();

		// helper functions
		const combineWithFilter = propsTree => (
			filter ? propsTree.combine(filterPropsTree) : propsTree);
		const combineWithOrder = propsTree => (
			order ? propsTree.combine(orderPropsTree) : propsTree);

		// build the records queries
		if (selectedPropPatterns) {

			// build and debranch the selected properties tree
			const selectPropsBranches = propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'select', baseValueExprCtx, null,
				selectedPropPatterns);

			// check if multiple branches
			if (selectPropsBranches.length > 1) {

				// check if ranged
				if (range) {

					// add filter and order properties to the tree
					const idsPropsTree = combineWithOrder(
						combineWithFilter(selectPropsBranches[0]));

					// build ids query tree
					const idsQueryTree = queryTreeBuilder.buildIdsOnly(
						dbDriver, recordTypes, idsPropsTree);

					// assemble ranged ids query
					const idsQuery = dbDriver.makeRangedSelect(
						idsQueryTree.assembleSelect(
							filter, order, this._paramsHandler),
						range.offset, range.limit);

					// create anchor table
					const anchorTable = 'q_' + idsQueryTree.table;
					dbDriver.makeSelectIntoTempTable(
						idsQuery, anchorTable,
						this._preStatements, this._postStatements);

					// build branch queries
					selectPropsBranches.forEach(selectPropsBranch => {

						// add order properties to the tree
						selectPropsBranch = combineWithOrder(selectPropsBranch);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectPropsBranch,
							anchorTable);

						// assemble the query
						this._queries.push(
							queryTree.assembleSelect(
								null, order, this._paramsHandler));
					});

				} else { // multi-branch, not ranged

					// add filter properties to the tree
					const idsPropsTree = combineWithFilter(
						selectPropsBranches[0]);

					// build ids query tree
					const idsQueryTree = queryTreeBuilder.buildIdsOnly(
						dbDriver, recordTypes, idsPropsTree);

					// assemble ids query
					const idsQuery = idsQueryTree._assembleSelect(
						filter, null, this._paramsHandler);

					// create anchor table
					const anchorTable = 'q_' + idsQueryTree.table;
					dbDriver.makeSelectIntoTempTable(
						idsQuery, anchorTable,
						this._preStatements, this._postStatements);

					// build branch queries
					selectPropsBranches.forEach(selectPropsBranch => {

						// add order properties to the tree
						selectPropsBranch = combineWithOrder(selectPropsBranch);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectPropsBranch,
							anchorTable);

						// assemble the query
						this._queries.push(
							queryTree.assembleSelect(
								null, order, this._paramsHandler));
					});
				}

			} else { // single branch

				// add filter and order properties to the tree
				const selectPropsTree = combineWithOrder(
					combineWithFilter(selectPropsBranches[0]));

				// check if ranged
				if (range) {

					// check if expanding children
					if (selectPropsTree.hasExpandingChild()) {

						// build ids query tree
						const idsQueryTree = queryTreeBuilder.buildIdsOnly(
							dbDriver, recordTypes, selectPropsTree);

						// assemble ranged ids query
						const idsQuery = dbDriver.makeRangedSelect(
							idsQueryTree.assembleSelect(
								filter, order, this._paramsHandler),
							range.offset, range.limit);

						// create anchor table
						const anchorTable = 'q_' + idsQueryTree.table;
						dbDriver.makeSelectIntoTempTable(
							idsQuery, anchorTable,
							this._preStatements, this._postStatements);

						// build anchored query tree
						const queryTree = queryTreeBuilder.buildAnchored(
							dbDriver, recordTypes, selectPropsTree,
							anchorTable);

						// assemble the query
						this._queries.push(
							queryTree.assembleSelect(
								null, order, this._paramsHandler));

					} else { // single branch, ranged, not expanding

						// build query tree
						const queryTree = queryTreeBuilder.buildDirect(
							dbDriver, recordTypes, selectPropsTree);

						// assemble the query
						const query = queryTree.assembleSelect(
							filter, order, this._paramsHandler);

						// make ranged
						this._queries.push(dbDriver.makeRangedSelect(
							query, range.offset, range.limit));
					}

				} else { // single branch, not ranged

					// build query tree
					const queryTree = queryTreeBuilder.buildDirect(
						dbDriver, recordTypes, selectPropsTree);

					// assemble the query
					this._queries.push(
						queryTree.assembleSelect(
							filter, order, this._paramsHandler));
				}
			}
		}
	}

	/**
	 * Execute the query.
	 *
	 * @param {(module:x2node-queries~Transaction|*)} tx The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the query execution.
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
	execute(tx, filterParams) {

		// part of a transaction?
		const hasTx = (tx instanceof Transaction);

		// make sure the transaction is active
		if (hasTx && !tx.isActive())
			throw new common.X2UsageError('The transaction is inactive.');

		// create query execution context
		const ctx = {
			connection: (hasTx ? tx.connection: tx),
			superPropsParser: null,
			recordsParser: null,
			executePostStatements: false
		};

		// determine if needs to be wrapped in a transaction
		const wrapInTx = (
			!hasTx && (
				(
					this._superPropsQueries.length +
						this._preStatements.length +
						this._queries.length +
						this._postStatements.length
				) > 1)
		);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (wrapInTx)
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						this._log('starting transaction');
						this._dbDriver.startTransaction(
							ctx.connection, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error starting transaction', err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error('error starting transaction', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);

		// queue up super property queries
		this._superPropsQueries.forEach(query => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const parser = rsparser.createResultSetParser(
							this._recordTypes, this._superTypeName);
						const sql = this._replaceParams(query, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onHeader(fieldNames) {
									parser.init(fieldNames);
								},
								onRow(row) {
									parser.feedRow(row);
								},
								onSuccess() {
									if (ctx.superPropsParser)
										ctx.superPropsParser.merge(parser);
									else
										ctx.superPropsParser = parser;
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + query + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// always execute post-statement after this point
		resPromise = resPromise.then(
			() => {
				ctx.executePostStatements = true;
			},
			err => Promise.reject(err)
		);

		// queue up pre-statements
		this._preStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const sql = this._replaceParams(stmt, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + stmt + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// queue up main queries
		this._queries.forEach(query => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						const parser = rsparser.createResultSetParser(
							this._recordTypes, this._recordTypeName);
						const sql = this._replaceParams(query, filterParams);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onHeader(fieldNames) {
									parser.init(fieldNames);
								},
								onRow(row) {
									parser.feedRow(row);
								},
								onSuccess() {
									if (ctx.recordsParser)
										ctx.recordsParser.merge(parser);
									else
										ctx.recordsParser = parser;
									resolve();
								},
								onError(err) {
									common.error(
										'error executing query [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing query [' + query + ']', err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		});

		// queue up post-statements
		const executePostStatement = stmt => new Promise((resolve, reject) => {
			if (!ctx.executePostStatements)
				return resolve();
			try {
				const sql = this._replaceParams(stmt, filterParams);
				this._log('executing SQL: ' + sql);
				this._dbDriver.execute(
					ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(
								'error executing query [' + sql + ']', err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error('error executing query [' + stmt + ']', err);
				reject(err);
			}
		});
		this._postStatements.forEach(stmt => {
			resPromise = resPromise.then(
				() => executePostStatement(stmt),
				err => executePostStatement(stmt).then(
					() => Promise.reject(err),
					() => Promise.reject(err)
				)
			);
		});

		// finish transaction if necessary
		if (wrapInTx)
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					try {
						this._log('committing transaction');
						this._dbDriver.commitTransaction(
							ctx.connection, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error committing transaction', err);
									this._log(
										'rolling back transaction after' +
											' failed commit');
									this._dbDriver.rollbackTransaction(
										ctx.connection, {
											onSuccess() {
												reject(err);
											},
											onError(rollbackErr) {
												common.error(
													'error rolling transaction' +
														' back after failed' +
														' commit',
													rollbackErr);
												reject(err);
											}
										}
									);
								}
							}
						);
					} catch (err) {
						common.error('error committing transaction', err);
						this._log(
							'rolling back transaction after failed commit');
						this._dbDriver.rollbackTransaction(
							ctx.connection, {
								onSuccess() {
									reject(err);
								},
								onError(rollbackErr) {
									common.error(
										'error rolling transaction back after' +
											' failed commit', rollbackErr);
									reject(err);
								}
							}
						);
					}
				}),
				err => new Promise((resolve, reject) => {
					try {
						this._log('rolling back transaction');
						this._dbDriver.rollbackTransaction(
							ctx.connection, {
								onSuccess() {
									reject(err);
								},
								onError(rollbackErr) {
									common.error(
										'error rolling transaction back',
										rollbackErr);
									reject(err);
								}
							}
						);
					} catch (rollbackErr) {
						common.error(
							'error rolling transaction back', rollbackErr);
						reject(err);
					}
				})
			);

		// build the final result object
		resPromise = resPromise.then(
			() => {
				const res = new Object();
				if (ctx.superPropsParser) {
					const superRec = ctx.superPropsParser.records[0];
					for (let superPropName in superRec)
						res[superPropName] = superRec[superPropName];
				} else {
					res.recordTypeName = this._recordTypeName;
				}
				if (ctx.recordsParser) {
					res.records = ctx.recordsParser.records;
					const refRecs = ctx.recordsParser.referredRecords;
					if (Object.keys(refRecs).length > 0)
						res.referredRecords = refRecs;
				}
				return res;
			},
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}

	/**
	 * Replace parameter placeholders in the specified SQL statement with the
	 * corresponding values.
	 *
	 * @private
	 * @param {string} stmtText SQL statement text with parameter placeholders.
	 * Each placeholder has format "?{ref}" where "ref" is the parameter
	 * reference in the query's filter parameters handler.
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
					res += this._paramsHandler.paramSql(
						this._dbDriver, params, m[3]);
				}
			}
		}
		res += stmtText.substring(lastMatchIndex);

		return res;
	}
}

// export the FetchQuery class
module.exports = FetchQuery;
