'use strict';

const common = require('x2node-common');
const rsparser = require('x2node-rsparser');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');
const rangeBuilder = require('./range-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');


// TODO: implement the locks

/////////////////////////////////////////////////////////////////////////////////
// COMMANDS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Abstract base command for executing a <code>SELECT</code> query and parsing
 * its result.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 * @abstract
 */
class AbstractFetchCommand {

	constructor(recordTypes, recordTypeName, query) {

		this._recordTypes = recordTypes;
		this._recordTypeName = recordTypeName;
		this._query = query;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		const parser = rsparser.getResultSetParser(
			this._recordTypes, this._recordTypeName);

		const query = this._query;
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				let sql;
				try {
					sql = ctx.replaceParams(query);
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeQuery(
						ctx.connection, sql, {
							onHeader(fieldNames) {
								parser.init(fieldNames);
							},
							onRow(row) {
								parser.feedRow(row);
							},
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						}
					);
				} catch (err) {
					common.error(`error executing SQL [${sql || query}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		).then(
			() => {
				this.onComplete(ctx, parser);
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Command for executing super-properties fetch.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractFetchCommand
 */
class ExecuteSuperPropertiesFetchCommand extends AbstractFetchCommand {

	constructor(recordTypes, superTypeName, query) {
		super(recordTypes, superTypeName, query);
	}

	onComplete(ctx, parser) {

		ctx.addSuperProps(parser);
	}
}

/**
 * Command for executing main records fetch.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractFetchCommand
 */
class ExecuteMainRecordsFetchCommand extends AbstractFetchCommand {

	constructor(recordTypes, recordTypeName, query) {
		super(recordTypes, recordTypeName, query);
	}

	onComplete(ctx, parser) {

		ctx.addRecords(parser);
	}
}


/////////////////////////////////////////////////////////////////////////////////
// EXECUTION CONTEXT
/////////////////////////////////////////////////////////////////////////////////

/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 */
class FetchDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor, filterParams) {
		super(dbo, txOrCon, actor, filterParams);

		this._superPropsParser = null;
		this._recordsParser = null;
	}

	addSuperProps(parser) {

		if (this._superPropsParser)
			this._superPropsParser.merge(parser);
		else
			this._superPropsParser = parser;
	}

	addRecords(parser) {

		if (this._recordsParser)
			this._recordsParser.merge(parser);
		else
			this._recordsParser = parser;
	}

	getResult() {

		const res = new Object();

		if (this._superPropsParser) {
			const superRec = this._superPropsParser.records[0];
			for (let superPropName in superRec)
				res[superPropName] = superRec[superPropName];
		} else {
			res.recordTypeName = this._dbo._recordTypeName;
		}

		if (this._recordsParser) {
			res.records = this._recordsParser.records;
			const refRecs = this._recordsParser.referredRecords;
			if (Object.keys(refRecs).length > 0)
				res.referredRecords = refRecs;
		}

		return res;
	}
}


/////////////////////////////////////////////////////////////////////////////////
// THE DBO
/////////////////////////////////////////////////////////////////////////////////

/**
 * Order specification for the anchor table.
 *
 * @private
 * @constant {module:x2node-dbos~RecordsOrder}
 */
const ANCHOR_ORDER = {
	elements: [{
		translate() { return 'q.ord'; }
	}]
};

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
	 * @param {Iterable.<string>} [selectedPropPatterns] Selected record property
	 * patterns, or nothing if records should not be fetched.
	 * @param {Array.<string>} [selectedSuperProps] Selected super-property
	 * names, or nothing if no super-properties need to be fetched.
	 * @param {Array.<Array>} [filterSpec] Optional specification of the filter
	 * to apply to the selected records.
	 * @param {Array.<string>} [orderSpec] Optional order specification to apply
	 * to the selected records.
	 * @param {Array.<number>} [rangeSpec] Optional range specification to apply
	 * to the selected records.
	 * @param {string} [lockType] Lock type. Either "shared" or "exclusive".
	 * @throws {module:x2node-common.X2UsageError} If the record types library is
	 * not suitable for the specified query.
	 * @throws {module:x2node-common.X2SyntaxError} If the provided query
	 * specification is invalid.
	 */
	constructor(
		dbDriver, recordTypes, recordTypeName, selectedPropPatterns,
		selectedSuperProps, filterSpec, orderSpecs, rangeSpec, lockType) {
		super(dbDriver, recordTypes);

		// save the basics
		this._lockType = lockType;

		// get the basics
		const recordTypeDesc = recordTypes.getRecordTypeDesc(recordTypeName);
		const superTypeName = recordTypeDesc.superRecordTypeName;
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			superTypeName).getPropertyDesc('records');

		// create base value expressions context
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);

		// build top records filter and the tree of properties used in it
		const filter = (filterSpec && filterBuilder.buildFilter(
			recordTypes, baseValueExprCtx, [ ':and', filterSpec ]));
		const filterPropsTree = (
			filter && propsTreeBuilder.buildPropsTreeBranches(
				recordTypes, recordsPropDesc, 'where', baseValueExprCtx, '',
				filter.usedPropertyPaths, {
					noWildcards: true,
					noAggregates: true,
					ignoreScopedOrders: true,
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

		// the operation commands sequence
		this._commands = new Array();

		// build super properties queries
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
						ignoreScopedOrders: true,
						noScopedFilters: true
					})[0]);

			// build query for each branch
			superPropsBranches.forEach(superPropsBranch => {

				// build query tree
				const queryTree = queryTreeBuilder.forSuperPropsQuery(
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

				// generate the SQL and add the fetch command
				this._commands.push(new ExecuteSuperPropertiesFetchCommand(
					recordTypes, superTypeName,
					this._assembleSelect(queryTree, combinedFilter, null).toSql()
				));
			});
		}

		// build records queries:

		// no queries if nothing is selected, we are done here
		if (!selectedPropPatterns)
			return;

		// helper functions
		const combineWithFilter = propsTree => (
			filter ? propsTree.combine(filterPropsTree) : propsTree);
		const combineWithOrder = propsTree => (
			order ? propsTree.combine(orderPropsTree) : propsTree);

		// build and debranch the selected properties tree
		const selectPropsBranches = propsTreeBuilder.buildPropsTreeBranches(
			recordTypes, recordsPropDesc, 'select', baseValueExprCtx, null,
			selectedPropPatterns);

		// check if multiple branches
		if (selectPropsBranches.length > 1) {

			// add filter and order properties to the tree
			const idsPropsTree = combineWithOrder(
				combineWithFilter(selectPropsBranches[0]));

			// build ids query tree
			const idsQueryTree = queryTreeBuilder.forIdsOnlyQuery(
				dbDriver, recordTypes, idsPropsTree);

			// add load anchor table command
			this._createLoadAnchorTableCommand(
				this._commands, idsQueryTree, filter, order, range, 'shared');

			// build branch queries
			const anchorTable = 'q_' + idsQueryTree.table;
			selectPropsBranches.forEach(selectPropsBranch => {

				// build anchored query tree
				const queryTree = queryTreeBuilder.forAnchoredQuery(
					dbDriver, recordTypes, 'select', true,
					selectPropsBranch, anchorTable);

				// assemble the SQL and add the fetch command
				this._commands.push(new ExecuteMainRecordsFetchCommand(
					recordTypes, recordTypeName,
					this._assembleSelect(queryTree, null, ANCHOR_ORDER).toSql()
				));
			});

		} else { // single branch

			// add filter and order properties to the tree
			const selectPropsTree = combineWithOrder(
				combineWithFilter(selectPropsBranches[0]));

			// check if ranged
			if (range) {

				// check if expanding children
				if (selectPropsTree.hasExpandingChild()) {

					// build ids query tree
					const idsQueryTree = queryTreeBuilder.forIdsOnlyQuery(
						dbDriver, recordTypes, selectPropsTree);

					// add load anchor table command
					this._createLoadAnchorTableCommand(
						this._commands, idsQueryTree, filter, order, range,
						'shared');

					// build anchored query tree
					const anchorTable = 'q_' + idsQueryTree.table;
					const queryTree = queryTreeBuilder.forAnchoredQuery(
						dbDriver, recordTypes, 'select', true,
						selectPropsTree, anchorTable);

					// assemble the SQL and add the fetch command
					this._commands.push(new ExecuteMainRecordsFetchCommand(
						recordTypes, recordTypeName,
						this._assembleSelect(queryTree, null, ANCHOR_ORDER)
							.toSql()
					));

				} else { // single branch, ranged, not expanding

					// build query tree
					const queryTree = queryTreeBuilder.forDirectQuery(
						dbDriver, recordTypes, 'select', true,
						selectPropsTree);

					// assemble the SQL
					const query = this._assembleSelect(
						queryTree, filter, order).toSql();

					// make it ranged and add the fetch command
					this._commands.push(new ExecuteMainRecordsFetchCommand(
						recordTypes, recordTypeName, dbDriver.makeRangedSelect(
							query, range.offset, range.limit)));
				}

			} else { // single branch, not ranged

				// build query tree
				const queryTree = queryTreeBuilder.forDirectQuery(
					dbDriver, recordTypes, 'select', true, selectPropsTree);

				// assemble the SQL and add the fetch command
				this._commands.push(new ExecuteMainRecordsFetchCommand(
					recordTypes, recordTypeName,
					this._assembleSelect(queryTree, filter, order).toSql()
				));
			}
		}
	}

	/**
	 * Fetch DBO execution result object. In addition to the basic result
	 * properties will include a property for each requested super-aggregate.
	 *
	 * @typedef {Object} module:x2node-dbos~FetchDBO~Result
	 * @property {string} recordTypeName Fetched record type name.
	 * @property {Array.<Object>} records Fetched records, or empty array if none
	 * matched.
	 * @property {Object.<string,Object>} [referredRecords] Fetched referred
	 * records by reference.
	 */

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {?module:x2node-common.Actor} actor Actor executing the DBO.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<module:x2node-dbos~FetchDBO~Result>} The fetch result
	 * promise.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, actor, filterParams) {

		return this._executeCommands(new FetchDBOExecutionContext(
			this, txOrCon, actor, filterParams));
	}
}

// export the class
module.exports = FetchDBO;
