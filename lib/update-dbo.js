'use strict';

const common = require('x2node-common');
const patches = require('x2node-patch');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const FetchDBO = require('./fetch-dbo.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');



/**
 * Abstract operation sequence command.
 *
 * @private
 * @abstract
 */
class Command {

	constructor() {}
}

/**
 * Command for updating column in a table.
 *
 * @private
 */
class UpdateColumnCommand extends Command {

	constructor(columnInfo, anchors, valueExpr) {
		super();

		this._columnInfo = columnInfo;
		this._anchors = anchors;

		this._sets = [
			{
				columnName: columnInfo.columnName,
				value: valueExpr
			}
		];
	}

	execute(promiseChain, ctx) {

		// build the anchor part of the WHERE clause
		const anchorsExpr = this._anchors.map(
			a => a.columnExpr + ' = ' + a.valueExpr).join(' AND ');

		// find the updated table node and build the statement
		const sql = ctx.updateQueryTree.forTableAlias(
			ctx.translationCtx, this._columnInfo.tableAlias,
			(propNode, tableDesc, tableChain) => {

				// flip the table chain
				if (tableChain.length > 0)
					tableChain[0].joinCondition = tableDesc.joinCondition;

				// build the UPDATE statement
				return ctx.dbDriver.buildUpdateWithJoins(
					tableDesc.tableName, tableDesc.tableAlias, this._sets,
					tableChain, anchorsExpr, false);
			}
		);

		return promiseChain.then(
			() => {
				console.log('=== UPDATE: [' + sql + ']');
				//...
			},
			err => Promise.reject(err)
		);
	}
}


/**
 * Operation execution context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~DBOExecutionContext
 * @implements module:x2node-patch.RecordPatchHandlers
 */
class UpdateDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor) {
		super(dbo, txOrCon, actor);

		this._updateQueryTree = dbo._updateQueryTree;
		this._translationCtx = this._updateQueryTree.getTopTranslationContext(
			dbo._paramsHandler);
		this._recordTypeDesc = dbo._recordTypeDesc;

		this._recordsUpdated = 0;
		this._testFailed = false;

		this._record = null;
		this._recordIdAnchor = null;
		this._commands = new Array();
	}

	get updateQueryTree() { return this._updateQueryTree; }

	get translationCtx() { return this._translationCtx; }

	/**
	 * Get update DBO execution result object.
	 *
	 * @returns {Object} The DBO result object.
	 */
	getResult() {

		return {
			recordsUpdated: this._recordsUpdated,
			testFailed: this._testFailed
		};
	}


	// execution cycle methods:

	/**
	 * Start processing of a record update.
	 *
	 * @param {Object} record The record data read from the database for update.
	 */
	startRecord(record) {

		this._record = record;

		const recIdPropName = this._recordTypeDesc.idPropertyName;
		this._recordIdAnchor = {
			columnExpr: this._translationCtx.translatePropPath(recIdPropName),
			valueExpr: this._dbDriver.sql(record[recIdPropName])
		};
	}

	/**
	 * Flush current record updates.
	 *
	 * @param {Promise} promiseChain The promise chain.
	 * @returns {Promise} The promise chain with record update operations added.
	 */
	flushRecord(promiseChain) {

		// check if any updates have been accumulated for the current record
		if (this._commands.length === 0)
			return promiseChain;

		// insert meta-info property update commands
		let metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'version');
		if (metaPropName)
			this._commands.unshift(new UpdateColumnCommand(
				this._translationCtx.getPropValueColumn(metaPropName),
				[ this._recordIdAnchor ],
				this._translationCtx.translatePropPath(metaPropName) + ' + 1'
			));
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationTimestamp');
		if (metaPropName)
			this._commands.unshift(new UpdateColumnCommand(
				this._translationCtx.getPropValueColumn(metaPropName),
				[ this._recordIdAnchor ],
				this._dbDriver.sql(this._executedOn.toISOString())
			));
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationActor');
		if (metaPropName)
			this._commands.unshift(new UpdateColumnCommand(
				this._translationCtx.getPropValueColumn(metaPropName),
				[ this._recordIdAnchor ],
				this._dbDriver.sql(this._actor.stamp)
			));

		// merge commands
		//...

		// initial result promise
		let resPromise = promiseChain;

		// queue up the commands
		for (let cmd of this._commands)
			resPromise = cmd.execute(resPromise, this);

		// update the updated records count
		resPromise = resPromise.then(
			() => {
				this._recordsUpdated++;
			},
			err => Promise.reject(err)
		);

		// reset the record commands
		this._commands.length = 0;

		// return the result promise
		return resPromise;
	}


	// patch handler methods:

	// process array/map element insert
	onInsert(op, ptr, value) {

		//...
	}

	// process array/map element removal
	onRemove(op, ptr) {

		//...
	}

	// process update
	onSet(op, ptr, value) {

		// build the anchors conditions
		const anchors = new Array();
		ptr.getValue(this._record, (prefixPtr, value, prefixDepth) => {
			if (prefixPtr.isRoot()) {
				anchors.push(this._recordIdAnchor);
			} else {
				//...
			}
		});

		// create the commands depending on the target type
		const propDesc = ptr.propDesc;
		if (propDesc.scalarValueType === 'object') {
			//...
		} else if (!ptr.collectionElement && propDesc.isArray()) {
			//...
		} else if (!ptr.collectionElement && propDesc.isMap()) {
			//...
		} else if (propDesc.isArray()) {
			//...
		} else if (propDesc.isMap()) {
			//...
		} else {
			this._commands.push(new UpdateColumnCommand(
				this._translationCtx.getPropValueColumn(ptr.propPath),
				anchors,
				this._dbDriver.sql(value)
			));
		}
	}

	// process test
	onTest(ptr, value, passed) {

		//...
	}
}


/**
 * Update database operation implementation (potentially a combination of SQL
 * <code>UPDATE</code>, <code>INSERT</code> and <code>DELETE</code> queries).
 *
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractDBO
 */
class UpdateDBO extends AbstractDBO {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using
	 * [DBOFactory]{@link module:x2node-dbos~DBOFactory}.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver The database driver.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {Array.<Object>} patch The JSON patch specification.
	 * @param {Array.<Array>} [filterSpec] Optional filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, recordTypeDesc, patch, filterSpec) {
		super(dbDriver);

		// save the basics
		this._recordTypes = recordTypes;
		this._recordTypeDesc = recordTypeDesc;

		// parse the patch
		this._patch = patches.buildJSONPatch(
			recordTypes, recordTypeDesc.name, patch);

		// assume actor not required until appropriate meta-info prop detected
		this._actorRequired = false;

		// add record meta-info props to the query
		const involvedPropPaths = new Set(this._patch.involvedPropPaths);
		[ 'version', 'modificationTimestamp', 'modificationActor' ]
			.forEach(r => {
				const propName = recordTypeDesc.getRecordMetaInfoPropName(r);
				if (propName) {
					involvedPropPaths.add(propName);
					if (r === 'modificationActor')
						this._actorRequired = true;
				}
			});

		// build the initial fetch DBO
		this._fetchDBO = new FetchDBO(
			dbDriver, recordTypes, recordTypeDesc.name, involvedPropPaths, [],
			filterSpec);

		// build update properties tree
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		const updatePropsTree = propsTreeBuilder.buildSimplePropsTree(
			recordTypes, recordsPropDesc, 'update', baseValueExprCtx,
			involvedPropPaths);

		// build update query tree
		this._updateQueryTree = queryTreeBuilder.forDirectQuery(
			dbDriver, recordTypes, 'update', false, updatePropsTree);
	}

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
	 * @returns {Promise.<Object>} The result promise, which resolves to the
	 * result object. The result object includes property
	 * <code>recordsUpdated</code>, which provides the number of records affected
	 * by the operation, including zero. It also includes Boolean property
	 * <code>testFailed</code>, which is <code>true</code> if the whole operation
	 * was rejected because one of the "test" patch operations failed. The
	 * promise is rejected with the error object of an error happens during the
	 * operation execution.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, actor, filterParams) {

		// check if actor is required
		if (this._actorRequired && !actor)
			throw new common.X2UsageError('Operation may not be anonymous.');

		// create operation execution context
		const ctx = new UpdateDBOExecutionContext(this, txOrCon, actor);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up initial fetch
		resPromise = resPromise.then(
			() => this._fetchDBO.execute(ctx.transaction, actor, filterParams),
			err => Promise.reject(err)
		);

		// queue up updates
		resPromise = resPromise.then(
			fetchResult => {
				let recordsChain = Promise.resolve();
				fetchResult.records.forEach(record => {
					ctx.startRecord(record);
					this._patch.apply(record, ctx);
					recordsChain = ctx.flushRecord(recordsChain);
				});
				return recordsChain;
			},
			err => Promise.reject(err)
		);

		// finish transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._endTx(resPromise, ctx);

		// build the final result object
		resPromise = resPromise.then(
			() => ctx.getResult(),
			err => Promise.reject(err)
		);

		// return the result promise chain
		return resPromise;
	}
}

// export the class
module.exports = UpdateDBO;
