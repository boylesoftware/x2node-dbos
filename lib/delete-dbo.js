'use strict';

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const filterBuilder = require('./filter-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');


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
class DeleteDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor, filterParams) {
		super(dbo, txOrCon, actor, filterParams);

		this._affectedRecordTypes = {};
	}

	affectedRows(numRows, stmtId) {

		if (numRows === 0)
			return;

		let recordTypeName = this._dbo._entangledRecordTypeUpdates[stmtId];
		if (recordTypeName !== undefined) {
			this._dbo._registerRecordTypeUpdate(recordTypeName);
			return;
		}

		recordTypeName = this._dbo._topRecordTypeDeletes[stmtId];
		if (recordTypeName !== undefined) {
			this._dbo._registerRecordTypeUpdate(recordTypeName);
			if (this._affectedRecordTypes.hasOwnProperty(recordTypeName))
				this._affectedRecordTypes[recordTypeName] += numRows;
			else
				this._affectedRecordTypes[recordTypeName] = numRows;
		}
	}

	getResult() {

		return this._affectedRecordTypes;
	}
}


/////////////////////////////////////////////////////////////////////////////////
// THE DBO
/////////////////////////////////////////////////////////////////////////////////

/**
 * Delete database operation implementation (SQL <code>DELETE</code> query).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class DeleteDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-dbos.RecordCollectionsMonitor} rcMonitor The record
	 * collections monitor.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {Array.<Array>} [filterSpec] Optional filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, rcMonitor, recordTypeDesc, filterSpec) {
		super(dbDriver, recordTypes, rcMonitor);

		// build base properties tree
		const recordProps = new Set();
		this._collectRecordProperties('', recordTypeDesc, recordProps);
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const basePropsTree = propsTreeBuilder.buildSimplePropsTree(
			recordTypes, recordsPropDesc, 'delete', baseValueExprCtx,
			recordProps);

		// build filter and properties tree for it
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

		// build combined properties tree
		const propsTree = (
			filter ? basePropsTree.combine(filterPropsTree) : basePropsTree);

		// the operation commands sequence
		this._commands = new Array();

		// decide what strategy to use and build initial table chain
		let queryTree, useFilter;
		if (!filter || (
			(filter.usedPropertyPaths.size === 1) &&
				filter.usedPropertyPaths.has(recordTypeDesc.idPropertyName)) ||
			this._isSingleDelete(recordTypeDesc)) {

			// use filter in the deletes
			useFilter = true;

			// build query tree
			queryTree = queryTreeBuilder.forDirectQuery(
				dbDriver, recordTypes, 'delete', false, propsTree);

		} else { // use anchor table

			// filter is used in the ids query
			useFilter = false;

			// build ids only query tree
			const idsQueryTree = queryTreeBuilder.forIdsOnlyQuery(
				dbDriver, recordTypes, propsTree);

			// add load anchor table command
			this._commands.push(this._createLoadAnchorTableCommand(
				idsQueryTree, filter, null, null, 'exclusive'));

			// build anchored query tree
			queryTree = queryTreeBuilder.forAnchoredQuery(
				dbDriver, recordTypes, 'delete', false, propsTree,
				`q_${idsQueryTree.table}`
			);
		}

		// translate the filter for the WHERE clauses
		const translationCtx = queryTree.getTopTranslationContext(
			this._paramsHandler);
		const filterExpr = (
			useFilter && filter && filter.translate(translationCtx));
		const filterExprParen = (
			useFilter && filter && filter.needsParen('AND'));

		// find all shared link tables
		const sharedLinkTables = new Object();
		for (let propName of recordTypeDesc.allPropertyNames) {
			const propDesc = recordTypeDesc.getPropertyDesc(propName);
			if (propDesc.isEntangled() && !propDesc.isView())
				sharedLinkTables[propDesc.table] = propDesc;
		}

		// walk the tree and build the delete statements
		const refTables = new Array();
		const usedRefs = new Set();
		this._topRecordTypeDeletes = new Array();
		this._entangledRecordTypeUpdates = new Array();
		queryTree.walk(translationCtx, (propNode, tableDesc) => {

			// collect tables used in the WHERE clause
			if ((propNode.path !== '') && propNode.isUsedIn('where') &&
				!propNode.isUsedIn('delete')) {
				refTables.push(tableDesc);
				usedRefs.add(tableDesc.tableAlias);
			}

		}).walkReverse(translationCtx, (propNode, tableDesc, tableChain) => {

			// delete from the table
			if (propNode.isUsedIn('delete')) {

				// check if deleting from shared link table
				const entangledPropDesc = sharedLinkTables[tableDesc.tableName];
				if (entangledPropDesc) {

					// check if has anything to update and build SET clause
					const entangledRecordTypeDesc =
						entangledPropDesc.nestedProperties;
					const sets = AbstractDBO.getModificationMetaInfoSets(
						null, entangledRecordTypeDesc, 'e');
					if (sets.length > 0) {

						// register entangled record type update
						this._entangledRecordTypeUpdates[this._commands.length] =
							entangledRecordTypeDesc.name;

						// add entangled record type table join
						tableChain[0].joinCondition = 'e.' +
							entangledRecordTypeDesc.getPropertyDesc(
								entangledRecordTypeDesc.idPropertyName).column +
							' = ' + tableDesc.tableAlias + '.' +
							entangledPropDesc.column;

						// build UPDATE statement for entangled records
						this._commands.push(this._createExecuteStatementCommand(
							dbDriver.buildUpdateWithJoins(
								entangledRecordTypeDesc.table, 'e', sets,
								refTables.concat(
									tableChain.filter(
										td => !usedRefs.has(td.tableAlias)),
									tableDesc
								),
								filterExpr, filterExprParen),
							this._commands.length
						));
					}
				}

				// register deletion of top record type records
				const childrenContainer = propNode.childrenContainer;
				if ((childrenContainer !== null) &&
					childrenContainer.isRecordType()) {
					this._topRecordTypeDeletes[this._commands.length] =
						childrenContainer.recordTypeName;
				}

				// flip the reference tables chain
				if (tableChain.length > 0)
					tableChain[0].joinCondition = tableDesc.joinCondition;

				// build the DELETE statement
				this._commands.push(this._createExecuteStatementCommand(
					dbDriver.buildDeleteWithJoins(
						tableDesc.tableName, tableDesc.tableAlias,
						refTables.concat(
							tableChain.filter(
								td => !usedRefs.has(td.tableAlias))
						),
						filterExpr, filterExprParen),
					this._commands.length
				));
			}
		});

		// add record collections monitor notification command
		this._commands.push(this._createNotifyRecordCollectionsMonitorCommand());
	}

	/**
	 * Recursively collect all property paths to include in the query tree to
	 * cover all tables involved in the records deletion.
	 *
	 * @private
	 * @param {string} basePathPrefix Path prefix, empty or ending with a dot.
	 * @param {module:x2node-records~PropertiesContainer} container Properties
	 * container to recursively process.
	 * @param {Set.<string>} recordProps Set, to which to add identified property
	 * paths.
	 */
	_collectRecordProperties(basePathPrefix, container, recordProps) {

		let hasThisLevel = false;

		if (container.idPropertyName) {
			recordProps.add(basePathPrefix + container.idPropertyName);
			hasThisLevel = true;
		}

		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.isView() || propDesc.isCalculated())
				continue;
			if ((propDesc.scalarValueType === 'object') || (
				propDesc.reverseRefPropertyName &&
					!propDesc.isWeakDependency())) {
				this._collectRecordProperties(
					basePathPrefix + propName + '.', propDesc.nestedProperties,
					recordProps);
			} else if (propDesc.table || !hasThisLevel) {
				recordProps.add(basePathPrefix + propName);
				hasThisLevel = true;
			}
		}
	}

	/**
	 * Tell if the operation can be completed in a single <code>DELETE</code>
	 * statement.
	 *
	 * @private
	 * @param {module:x2node-records~PropertiesContainer} container Properties
	 * container to recursively process.
	 * @returns {boolean} <code>true</code> if single statement delete operation.
	 */
	_isSingleDelete(container) {

		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);
			if (propDesc.table || (
				propDesc.reverseRefPropertyName && propDesc.isWeakDependency()))
				return false;
			if ((propDesc.scalarValueType === 'object') &&
				!this._isSingleDelete(propDesc.nestedProperties))
				return false;
		}

		return true;
	}

	/**
	 * Delete DBO execution result object. The result object's properties are
	 * names of record types, records of which were actually deleted, and the
	 * values are the numbers of records of the type deleted (zeros are not
	 * included).
	 *
	 * @typedef module:x2node-dbos~DeleteDBO~Result
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
	 * @returns {Promise.<module:x2node-dbos~DeleteDBO~Result>} The operation
	 * result object promise.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, actor, filterParams) {

		return this._executeCommands(new DeleteDBOExecutionContext(
			this, txOrCon, actor, filterParams));
	}
}

// export the class
module.exports = DeleteDBO;
