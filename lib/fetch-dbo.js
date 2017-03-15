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
const AbstractDBO = require('./abstract-dbo.js');


/**
 * Filter parameters handler used by a DBO.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class FilterParamsHandler {

	/**
	 * Create new parameters handler for a DBO.
	 */
	constructor() {

		this._nextParamRef = 0;

		this._params = new Map();
	}

	/**
	 * Add parameter mapping.
	 *
	 * @param {string} paramName Parameter name.
	 * @param {function} valueFunc Function that given the parameter value
	 * provided with DBO call returns the ES literal for the value to include
	 * in the database statement.
	 * @returns {string} Parameter placeholder to include in the SQL in the
	 * form of "?{ref}", where "ref" is the parameter reference assigned by the
	 * handler.
	 */
	addParam(paramName, valueFunc) {

		const paramRef = String(this._nextParamRef++);

		this._params.set(paramRef, {
			name: paramName,
			valueFunc: valueFunc
		});

		return '?{' + paramRef + '}';
	}

	/**
	 * Get parameter value SQL expression.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {*} paramValue Parameter value from the DBO specification.
	 * @param {function} valueFunc Parameter value function (see
	 * <code>addParam</code> method).
	 * @param {string} paramName Parameter name (for error reporting only).
	 * @returns {string} Parameter value SQL expression to include in the
	 * database statement.
	 */
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

	/**
	 * Get parameter value SQL expression given the parameter reference.
	 *
	 * @param {module:x2node-dbos.DBDriver} dbDriver DB driver.
	 * @param {Object.<string,*>} filterParams Parameters provided with the DBO
	 * execution call.
	 * @param {string} paramRef Parameter reference from the SQL.
	 * @returns {string} Parameter value SQL expression to include in the
	 * database statement.
	 */
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
 * Fetch database operation implementation (SQL <code>SELECT</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class FetchDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} recordTypeName Fetched record type name.
	 * @param {Array.<string>} [selectedPropPatterns] Selected record property
	 * patterns, or nothing if records should not be fetched.
	 * @param {Array.<string>} [selectedSuperProps] Selected super-property
	 * names, or nothing if no super-properties need to be fetched.
	 * @param {Array.<Array>} [filterSpec] Optional specification of the filter
	 * to apply to the selected records.
	 * @param {Array.<string>} [orderSpec] Optional order specification to apply
	 * to the selected records.
	 * @param {Array.<number>} [rangeSpec] Optional range specification to apply
	 * to the selected records.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the underlying record types library is not
	 * suitable for it.
	 */
	constructor(
		dbDriver, recordTypes, recordTypeName, selectedPropPatterns,
		selectedSuperProps, filterSpec, orderSpecs, rangeSpec) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;
		this._recordTypeName = recordTypeName;
		const recordTypeDesc = recordTypes.getRecordTypeDesc(recordTypeName);
		this._superTypeName = recordTypeDesc.superRecordTypeName;

		// create base value expressions context
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);

		// get records property descriptor
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			this._superTypeName).getPropertyDesc('records');

		// build top records filter and the tree of properties used in it
		const filter = (filterSpec && filterBuilder.buildFilter(
			recordTypes, baseValueExprCtx, [ ':and', filterSpec ]));
		const filterPropsTree = (
			filter && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'where', baseValueExprCtx, '',
				filter.usedPropertyPaths, {
					noWildcards: true,
					noAggregates: true,
					ingnoreScopedOrders: true,
					noScopedFilters: true
				})[0]);

		// build top records order and the tree of properties used in it
		const order = (orderSpecs && orderBuilder.buildOrder(
			baseValueExprCtx, orderSpecs));
		const orderPropsTree = (
			order && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'orderBy', baseValueExprCtx, '',
				order.usedPropertyPaths, {
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

			// build and debranch the selected super-properties tree
			const superPropsBranches =
				propsTreeBuilder.buildSuperPropsTreeBranches(
					recordTypes, recordTypeDesc, selectedSuperProps);

			// create filter for the super queries
			const superFilter = filter && filter.rebase('records');
			const superFilterPropsTree = (
				superFilter && propsTreeBuilder.buildPropsTreeBranches(
					recordTypes, recordsPropDesc, 'where',
					superPropsBranches[0].getValueExpressionContext(), '',
					superFilter.usedPropertyPaths, {
						noWildcards: true,
						noAggregates: true,
						ingnoreScopedOrders: true,
						noScopedFilters: true
					})[0]);

			// build query for each branch
			superPropsBranches.forEach(superPropsBranch => {

				// build query tree
				const queryTree = queryTreeBuilder.buildSuperPropsQuery(
					dbDriver, recordTypes,
					(
						superFilterPropsTree ?
							superPropsBranch.combine(superFilterPropsTree) :
							superPropsBranch
					));

				// create filter
				let combinedFilter;
				if (queryTree.joinCondition && superFilter)
					combinedFilter = queryTree.joinCondition.conjoin(
						superFilter);
				else if (queryTree.joinCondition)
					combinedFilter = queryTree.joinCondition;
				else if (superFilter)
					combinedFilter = superFilter;

				// generate the SQL
				this._superPropsQueries.push(queryTree.assembleSelect(
					combinedFilter, null, this._paramsHandler));
			});
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

					// assemble ranged ids SELECT statement
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

						// assemble the SQL
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

					// assemble ids SELECT statement
					const idsQuery = idsQueryTree.assembleSelect(
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

						// assemble the SQL
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

						// assemble ranged ids SELECT statement
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

						// assemble the SQL
						this._queries.push(
							queryTree.assembleSelect(
								null, order, this._paramsHandler));

					} else { // single branch, ranged, not expanding

						// build query tree
						const queryTree = queryTreeBuilder.buildDirect(
							dbDriver, recordTypes, selectPropsTree);

						// assemble the SQL
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

					// assemble the SQL
					this._queries.push(
						queryTree.assembleSelect(
							filter, order, this._paramsHandler));
				}
			}
		}
	}

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} tx The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<Object>} The result promise, which resolves to the
	 * result object that has <code>records</code> and optionally
	 * <code>referredRecords</code> properties plus any requested
	 * super-aggregates. The promise is rejected with the error object of an
	 * error happens during the operation execution.
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

		// create operation execution context
		const ctx = {
			connection: (hasTx ? tx.connection: tx),
			superPropsParser: null,
			recordsParser: null,
			executePostStatements: false,
			rollbackOnError: wrapInTx
		};

		// create parameters resolver function
		const paramsResolver = paramRef => this._paramsHandler.paramSql(
			this._dbDriver, filterParams, paramRef);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up super property queries
		this._superPropsQueries.forEach(query => {
			resPromise = resPromise.then(
				() => new Promise((resolve, reject) => {
					let sql;
					try {
						const parser = rsparser.getResultSetParser(
							this._recordTypes, this._superTypeName);
						sql = this._replaceParams(query, paramsResolver);
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
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || query) + ']', err);
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
					let sql;
					try {
						sql = this._replaceParams(stmt, paramsResolver);
						this._log('executing SQL: ' + sql);
						this._dbDriver.execute(
							ctx.connection, sql, {
								onSuccess() {
									resolve();
								},
								onError(err) {
									common.error(
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || stmt) + ']', err);
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
					let sql;
					try {
						const parser = rsparser.getResultSetParser(
							this._recordTypes, this._recordTypeName);
						sql = this._replaceParams(query, paramsResolver);
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
										'error executing SQL [' + sql + ']',
										err);
									reject(err);
								}
							}
						);
					} catch (err) {
						common.error(
							'error executing SQL [' + (sql || query) + ']', err);
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
			let sql;
			try {
				sql = this._replaceParams(stmt, paramsResolver);
				this._log('executing SQL: ' + sql);
				this._dbDriver.execute(
					ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(
								'error executing SQL [' + sql + ']', err);
							reject(err);
						}
					}
				);
			} catch (err) {
				common.error('error executing SQL [' + (sql || stmt) + ']', err);
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
			resPromise = this._endTx(resPromise, ctx);

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
}

// export the class
module.exports = FetchDBO;
