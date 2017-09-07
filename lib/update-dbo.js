'use strict';

const common = require('x2node-common');
const pointers = require('x2node-pointers');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const FetchDBO = require('./fetch-dbo.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');


/////////////////////////////////////////////////////////////////////////////////
// COMMANDS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Pre-fetch command. When executed, fetches all matching records due for update
 * and sets them in the context.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class PrefetchCommand {

	constructor(fetchDBO) {

		this._fetchDBO = fetchDBO;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		return promiseChain.then(
			() => this._fetchDBO.execute(
				ctx.transaction, ctx.actor, ctx.filterParams),
			err => Promise.reject(err)
		).then(
			result => {
				ctx.setRecords(result.records);
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Update pre-fetched records command. When executed, goes over each record set
 * in the context, applies the patch, validates and flushes it to the database.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 */
class UpdateRecordsCommand {

	constructor(patch) {

		this._patch = patch;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		return promiseChain.then(
			() => {

				// pre-resolve updates sub-chain
				let recordsChain = Promise.resolve();

				// go over each record in the context
				for (let rec = ctx.nextRecord(); rec; rec = ctx.nextRecord()) {

					// add validator, if any
					if (ctx.beforePatchRecordValidator) {
						recordsChain = recordsChain.then(
							() => ctx.beforePatchRecordValidator(rec),
							err => Promise.reject(err)
						);
					}

					// add the patch
					recordsChain = recordsChain.then(
						() => this._patch.apply(rec, ctx),
						err => Promise.reject(err)
					);

					// add validator, if any
					if (ctx.afterPatchRecordValidator) {
						recordsChain = recordsChain.then(
							() => ctx.afterPatchRecordValidator(rec),
							err => Promise.reject(err)
						);
					}

					// add updates flush to the database
					recordsChain = recordsChain.then(
						() => ctx.flushRecord(),
						err => Promise.reject(err)
					);
				}

				// return record updates sub-chain
				return recordsChain;
			},
			err => Promise.reject(err)
		);
	}
}

/**
 * Abstract base for commands used to update the matched records.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 * @abstract
 */
class AbstractUpdateCommand {

	/**
	 * Create new command.
	 *
	 * @param {Object} propCtx Property context object.
	 */
	constructor(propCtx) {

		this._propCtx = propCtx;
	}

	/**
	 * Get reference tables for the command.
	 *
	 * @protected
	 */
	_getRefTables(tableDesc, tableChain) {

		// flip the table chain
		if (tableChain.length > 0)
			tableChain[0].joinCondition = tableDesc.joinCondition;

		// find the unique table in the chain
		const uniqueIdTableAlias = this._propCtx.uniqueIdColumnInfo.tableAlias;
		const uniqueIdTableIndex = tableChain.findIndex(
			t => (t.tableAlias === uniqueIdTableAlias));

		// no ref tables if unique id table is outside the chain
		if (uniqueIdTableIndex < 0)
			return null;

		// return the whole chain if starts with the unique id table
		if (uniqueIdTableIndex === 0)
			return tableChain;

		// cut the chain up to the unique id table and return the result
		return tableChain.slice(uniqueIdTableIndex);
	}
}

/**
 * Command for updating column in a table.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractUpdateCommand
 */
class UpdateColumnCommand extends AbstractUpdateCommand {

	constructor(propCtx, columnInfo, valueExpr, addlCondExpr) {
		super(propCtx);

		this._columnInfo = columnInfo;

		this._sets = [
			{
				columnName: columnInfo.columnName,
				value: valueExpr
			}
		];

		this._addlCondExpr = (addlCondExpr || null);
	}

	/**
	 * Attempt to merge another update command into this one.
	 *
	 * @param {module:x2node-dbos~UpdateColumnCommand} otherCmd The other update
	 * command.
	 * @returns {boolean} <code>true</code> if merged.
	 */
	merge(otherCmd) {

		// check if updates same table
		if (otherCmd._columnInfo.tableAlias !== this._columnInfo.tableAlias)
			return false;

		// check if same anchors
		if (otherCmd._propCtx.anchorsExpr !== this._propCtx.anchorsExpr)
			return false;

		// check if same additional condition
		if (otherCmd._addlCondExpr !== this._addlCondExpr)
			return false;

		// all good, import the sets
		otherCmd._sets.forEach(s => { this._sets.push(s); });

		// merged
		return true;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		// find the updated table node and build the UPDATE statement
		const sql = ctx.updateQueryTree.forTableAlias(
			ctx.translationCtx, this._columnInfo.tableAlias,
			(propNode, tableDesc, tableChain) => {

				// build the UPDATE statement
				return ctx.dbDriver.buildUpdateWithJoins(
					tableDesc.tableName, tableDesc.tableAlias, this._sets,
					this._getRefTables(tableDesc, tableChain),
					this._propCtx.anchorsExpr + (
						this._addlCondExpr ? ' AND ' + this._addlCondExpr : ''),
					false);
			}
		);

		// queue up execution of the UPDATE statement
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				try {
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeUpdate(ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(`error executing SQL [${sql}]`, err);
							reject(err);
						}
					});
				} catch (err) {
					common.error(`error executing SQL [${sql}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
}

/**
 * Command for clearing a simple value collection table.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractUpdateCommand
 */
class ClearSimpleCollectionCommand extends AbstractUpdateCommand {

	constructor(propCtx, tableAlias, addlCondExpr) {
		super(propCtx);

		this._tableAlias = tableAlias;
		this._addlCondExpr = addlCondExpr;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		// find the table node and build the DELETE statement
		const sql = ctx.updateQueryTree.forTableAlias(
			ctx.translationCtx, this._tableAlias,
			(propNode, tableDesc, tableChain) => {

				// build the DELETE statement
				return ctx.dbDriver.buildDeleteWithJoins(
					tableDesc.tableName, tableDesc.tableAlias,
					this._getRefTables(tableDesc, tableChain),
					this._propCtx.anchorsExpr + (
						this._addlCondExpr ? ' AND ' + this._addlCondExpr : ''),
					false);
			}
		);

		// queue up execution of the DELETE statement
		return promiseChain.then(
			() => new Promise((resolve, reject) => {
				try {
					ctx.log(`executing SQL: ${sql}`);
					ctx.dbDriver.executeUpdate(ctx.connection, sql, {
						onSuccess() {
							resolve();
						},
						onError(err) {
							common.error(`error executing SQL [${sql}]`, err);
							reject(err);
						}
					});
				} catch (err) {
					common.error(`error executing SQL [${sql}]`, err);
					reject(err);
				}
			}),
			err => Promise.reject(err)
		);
	}
}

/**
 * Command for populating a simple value array table.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractUpdateCommand
 */
class PopulateSimpleArrayCommand extends AbstractUpdateCommand {

	constructor(
		tableName, parentIdColumn, parentIdExpr, indexColumn, baseIndex,
		valueColumn, valueExprs) {
		super();

		this._tableName = tableName;
		this._parentIdColumn = parentIdColumn;
		this._parentIdExpr = parentIdExpr;
		this._indexColumn = indexColumn;
		this._baseIndex = baseIndex;
		this._valueColumn = valueColumn;
		this._valueExprs = valueExprs;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		// build the INSERT statements
		const sqls = this._valueExprs.map((valueExpr, i) => (
			'INSERT INTO ' + this._tableName + ' (' +
				this._parentIdColumn +
				(this._indexColumn ? ', ' + this._indexColumn : '') +
				', ' + this._valueColumn +
				') VALUES (' +
				this._parentIdExpr +
				(this._indexColumn ? ', ' + String(this._baseIndex + i) : '') +
				', ' + valueExpr + ')'
		));

		// queue up execution of the INSERT statements
		for (let sql of sqls) {
			promiseChain = promiseChain.then(
				() => new Promise((resolve, reject) => {
					try {
						ctx.log(`executing SQL: ${sql}`);
						ctx.dbDriver.executeInsert(ctx.connection, sql, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						});
					} catch (err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		}

		// return the resulting chain
		return promiseChain;
	}
}

/**
 * Command for populating a simple value map table.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractUpdateCommand
 */
class PopulateSimpleMapCommand extends AbstractUpdateCommand {

	constructor(
		tableName, parentIdColumn, parentIdExpr, keyColumn, valueColumn,
		keyValueExprs) {
		super();

		this._tableName = tableName;
		this._parentIdColumn = parentIdColumn;
		this._parentIdExpr = parentIdExpr;
		this._keyColumn = keyColumn;
		this._valueColumn = valueColumn;
		this._keyValueExprs = keyValueExprs;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		// build the INSERT statements
		const sqls = new Array();
		this._keyValueExprs.forEach(keyValueExprsPair => {
			sqls.push(
				'INSERT INTO ' + this._tableName + ' (' +
					this._parentIdColumn + ', ' + this._keyColumn +
					', ' + this._valueColumn + ') VALUES (' +
					this._parentIdExpr + ', ' + keyValueExprsPair[0] +
					', ' + keyValueExprsPair[1] + ')'
			);
		});

		// queue up execution of the INSERT statements
		for (let sql of sqls) {
			promiseChain = promiseChain.then(
				() => new Promise((resolve, reject) => {
					try {
						ctx.log(`executing SQL: ${sql}`);
						ctx.dbDriver.executeInsert(ctx.connection, sql, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						});
					} catch (err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		}

		// return the resulting chain
		return promiseChain;
	}
}

/**
 * Command for recursively clearing a table with nested objects (scalar or
 * collection).
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @extends module:x2node-dbos~AbstractUpdateCommand
 */
class ClearObjectsCommand extends AbstractUpdateCommand {

	constructor(propCtx, tableAlias) {
		super(propCtx);

		this._tableAlias = tableAlias;
	}

	// add command execution to the chain
	queueUp(promiseChain, ctx) {

		// find the table node and build the DELETE statements
		const sqls = new Array();
		ctx.updateQueryTree.walkReverse(
			ctx.translactionCtx, (propNode, tableDesc, tableChain) => {
				if (tableDesc.tableAlias.startsWith(this._tableAlias)) {

					// build the DELETE statement
					sqls.push(ctx.dbDriver.buildDeleteWithJoins(
						tableDesc.tableName, tableDesc.tableAlias,
						this._getRefTables(tableDesc, tableChain),
						this._propCtx.anchorsExpr, false));
				}
			});

		// queue up execution of the DELETE statements
		for (let sql of sqls) {
			promiseChain = promiseChain.then(
				() => new Promise((resolve, reject) => {
					try {
						ctx.log(`executing SQL: ${sql}`);
						ctx.dbDriver.executeUpdate(ctx.connection, sql, {
							onSuccess() {
								resolve();
							},
							onError(err) {
								common.error(
									`error executing SQL [${sql}]`, err);
								reject(err);
							}
						});
					} catch (err) {
						common.error(`error executing SQL [${sql}]`, err);
						reject(err);
					}
				}),
				err => Promise.reject(err)
			);
		}

		// return the resulting chain
		return promiseChain;
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
 * @implements module:x2node-patches.RecordPatchHandlers
 */
class UpdateDBOExecutionContext extends DBOExecutionContext {

	constructor(dbo, txOrCon, actor, filterParams, recordValidators) {
		super(dbo, txOrCon, actor, filterParams, new Object());

		// operation specific info
		this._updateQueryTree = dbo._updateQueryTree;
		this._translationCtx = this._updateQueryTree.getTopTranslationContext(
			dbo._paramsHandler);
		this._recordTypeDesc = dbo._recordTypeDesc;
		this._recordIdPropName = dbo._recordTypeDesc.idPropertyName;
		if ((typeof recordValidators) === 'function') {
			this._afterPatchRecordValidator = recordValidators;
		} else if (((typeof recordValidators) === 'object') &&
			(recordValidators !== null)) {
			this._beforePatchRecordValidator = recordValidators.beforePatch;
			this._afterPatchRecordValidator = recordValidators.afterPatch;
		}

		// whole operation result
		this._records = undefined;
		this._updatedRecordIds = new Array();
		this._testFailed = false;
		this._failedRecordIds = undefined;

		// context record data
		this._recordIndex = undefined;
		this._record = null;
		this._commands = new Array();
		this._recordTestFailed = null;
		this._recordEntangledUpdates = null;
	}

	/**
	 * Update query tree.
	 *
	 * @protected
	 * @member {module:x2node-dbos~QueryTreeNode}
	 * @readonly
	 */
	get updateQueryTree() { return this._updateQueryTree; }

	/**
	 * Translation context.
	 *
	 * @protected
	 * @member {module:x2node-dbos~TranslationContext}
	 * @readonly
	 */
	get translationCtx() { return this._translationCtx; }

	/**
	 * Record validator function called before applying the patch.
	 *
	 * @member {?module:x2node-dbos~UpdateDBO~recordValidator}
	 * @readonly
	 */
	get beforePatchRecordValidator() { return this._beforePatchRecordValidator; }

	/**
	 * Record validator function called after applying the patch.
	 *
	 * @member {?module:x2node-dbos~UpdateDBO~recordValidator}
	 * @readonly
	 */
	get afterPatchRecordValidator() { return this._afterPatchRecordValidator; }


	// execution cycle methods:

	/**
	 * Set matched records to the context before starting applying the updates.
	 *
	 * @param {Array.<Object>} records Matched records.
	 */
	setRecords(records) {

		this._records = records;
		this._recordIndex = -1;
	}

	/**
	 * Start processing next record update.
	 *
	 * @returns {Object} The record data read from the database for update, or
	 * <code>null</code> if no more records.
	 */
	nextRecord() {

		// check if no more records
		if (this._recordIndex === this._records.length - 1)
			return null;

		// set the current record in the context
		this._record = this._records[++this._recordIndex];

		// reset the context record commands
		this._commands.length = 0;
		this._recordTestFailed = false;
		this._recordEntangledUpdates = null;

		// return the record
		return this._record;
	}

	/**
	 * Flush current record updates.
	 *
	 * @returns {Promise} Promise of the flush completion, or nothing if no flush
	 * is required.
	 */
	flushRecord() {

		// check if "test" operation failed for the current record
		if (this._recordTestFailed) {
			this._testFailed = true;
			if (!this._failedRecordIds)
				this._failedRecordIds = new Array();
			this._failedRecordIds.push(this._record[this._recordIdPropName]);
			return;
		}

		// check if the current record needs any modification
		if (this._commands.length === 0)
			return;

		// the record was modified, save the modifications into the database:

		// register record type update
		this._dbo._registerRecordTypeUpdate(this._recordTypeDesc.name);

		// merge entangled record ids into the main registry
		if (this._recordEntangledUpdates) {
			for (let recordTypeName in this._recordEntangledUpdates) {
				this.addEntangledUpdates(
					recordTypeName,
					this._recordEntangledUpdates[recordTypeName]);
			}
		}

		// insert meta-info property update commands
		const rootPropCtx = this._getPropertyContext(
			pointers.parse(this._recordTypeDesc, ''));
		let metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationActor');
		if (metaPropName) {
			this._record[metaPropName] = this._actor.stamp;
			this._commands.unshift(new UpdateColumnCommand(
				rootPropCtx,
				this._translationCtx.getPropValueColumn(metaPropName),
				this._dbDriver.sql(this._actor.stamp)
			));
		}
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'modificationTimestamp');
		if (metaPropName) {
			const date = this._executedOn.toISOString();
			this._record[metaPropName] = date;
			this._commands.unshift(new UpdateColumnCommand(
				rootPropCtx,
				this._translationCtx.getPropValueColumn(metaPropName),
				this._dbDriver.sql(date)
			));
		}
		metaPropName = this._recordTypeDesc.getRecordMetaInfoPropName(
			'version');
		if (metaPropName) {
			this._commands.unshift(new UpdateColumnCommand(
				rootPropCtx,
				this._translationCtx.getPropValueColumn(metaPropName),
				this._dbDriver.sql(++this._record[metaPropName]) // locked
			));
		}

		// merge commands
		for (let i = 0; i < this._commands.length; i++) {
			const baseCmd = this._commands[i];
			if (baseCmd instanceof UpdateColumnCommand) {
				for (let j = i + 1; j < this._commands.length; j++) {
					const cmd = this._commands[j];
					if (!(cmd instanceof UpdateColumnCommand))
						break;
					if (baseCmd.merge(cmd))
						this._commands.splice(j--, 1);
				}
			}
		}

		// initial result promise
		let resPromise = Promise.resolve();

		// queue up the commands
		for (let cmd of this._commands)
			resPromise = cmd.queueUp(resPromise, this);

		// update the updated record ids list
		resPromise = resPromise.then(
			() => {
				this._updatedRecordIds.push(
					this._record[this._recordIdPropName]);
				this.clearGeneratedParams();
			},
			err => Promise.reject(err)
		);

		// return the result promise
		return resPromise;
	}

	/**
	 * Get update DBO execution result object.
	 *
	 * @returns {Object} The DBO result object.
	 */
	getResult() {

		return {
			records: this._records,
			updatedRecordIds: this._updatedRecordIds,
			testFailed: this._testFailed,
			failedRecordIds: this._failedRecordIds
		};
	}


	// patch handler methods:

	// process array/map element insert
	onInsert(op, ptr, newValue) {

		// get property context
		const propCtx = this._getPropertyContext(ptr);

		// update entanglements
		const propDesc = ptr.propDesc;
		if (propDesc.isEntangled() && ((typeof newValue) === 'string'))
			this._getEntangledRecordIds(propDesc).push(
				propDesc.nestedProperties.refToId(newValue));

		// create the commands depending on whether it's an array or map
		if (propDesc.isArray()) {

			// get element index column info
			const propIndColumnInfo =
				this._translationCtx.getPropValueColumn(
					ptr.propPath + '.$index');

			// make room for the new element and get new element index
			let newElementInd;
			if (propIndColumnInfo) {

				// determine base prop ctx (remove last anchor if nested object)
				const basePropCtx = (
					propDesc.scalarValueType === 'object' ?
						this._getPropertyContext(ptr.parent) : propCtx);

				// get number of elements in the array after insert
				const arrayLen = ptr.parent.getValue(this._record).length;

				// make room if not adding to the end of the array
				if (ptr.collectionElementIndex !== '-') {
					newElementInd = ptr.collectionElementIndex;
					const indColExpr = propIndColumnInfo.tableAlias + '.' +
						propIndColumnInfo.columnName;
					if (newElementInd === arrayLen - 2) {
						this._commands.push(new UpdateColumnCommand(
							basePropCtx, propIndColumnInfo, `${indColExpr} + 1`,
							`${indColExpr} >= ${newElementInd}`));
					} else {
						this._commands.push(new UpdateColumnCommand(
							basePropCtx, propIndColumnInfo,
							`${indColExpr} + ${arrayLen}`,
							`${indColExpr} >= ${newElementInd}`));
						this._commands.push(new UpdateColumnCommand(
							basePropCtx, propIndColumnInfo,
							`${indColExpr} - ${arrayLen - 1}`,
							`${indColExpr} >= ${arrayLen}`));
					}
				} else {
					newElementInd = arrayLen - 1;
				}
			}

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// insert new object
				this.addGeneratedParam(
					propCtx.parentIdPropPath, propCtx.parentIdValue);
				this._dbo._createInsertCommands(
					this._commands, propDesc.table,
					propDesc.parentIdColumn, propCtx.parentIdPropPath,
					propDesc, newElementInd, propDesc.nestedProperties,
					newValue);

			} else { // simple value

				// add new element
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				this._commands.push(new PopulateSimpleArrayCommand(
					propDesc.table, propDesc.parentIdColumn,
					this._dbDriver.sql(propCtx.parentIdValue),
					propDesc.indexColumn, newElementInd,
					propValColumnInfo.columnName,
					[ this._valueToSql(propDesc, newValue) ]
				));
			}

		} else { // map element

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// insert new object
				this.addGeneratedParam(
					propCtx.parentIdPropPath, propCtx.parentIdValue);
				this._dbo._createInsertCommands(
					this._commands, propDesc.table,
					propDesc.parentIdColumn, propCtx.parentIdPropPath,
					propDesc, ptr.collectionElementIndex,
					propDesc.nestedProperties, newValue);

			} else { // simple value

				// add new element
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				const propKeyColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$key');
				this._commands.push(new PopulateSimpleMapCommand(
					propDesc.table, propDesc.parentIdColumn,
					this._dbDriver.sql(propCtx.parentIdValue),
					propKeyColumnInfo.columnName,
					propValColumnInfo.columnName,
					[[
						this._dbo._makeMapKeySql(
							propDesc, ptr.collectionElementIndex),
						this._valueToSql(propDesc, newValue)
					]]
				));
			}
		}
	}

	// process array/map element removal
	onRemove(op, ptr, oldValue) {

		// get property context
		const propCtx = this._getPropertyContext(ptr, oldValue);

		// update entanglements
		const propDesc = ptr.propDesc;
		if (propDesc.isEntangled() && ((typeof oldValue) === 'string'))
			this._getEntangledRecordIds(propDesc).push(
				propDesc.nestedProperties.refToId(oldValue));

		// create the commands depending on whether it's an array or map
		if (propDesc.isArray()) {

			// get element index column info
			const propIndColumnInfo = this._translationCtx.getPropValueColumn(
				ptr.propPath + '.$index');

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing object
				const pidColumnInfo = this._translationCtx.getPropValueColumn(
					ptr.propPath + '.$parentId');
				this._commands.push(new ClearObjectsCommand(
					propCtx, pidColumnInfo.tableAlias));

				// shift element indexes left
				if (propIndColumnInfo) {
					const indColExpr = propIndColumnInfo.tableAlias + '.' +
						propIndColumnInfo.columnName;
					this._commands.push(new UpdateColumnCommand(
						this._getPropertyContext(ptr.parent), propIndColumnInfo,
						`${indColExpr} - 1`,
						`${indColExpr} > ${ptr.collectionElementIndex}`));
				}

			} else { // simple value

				// get value column info
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');

				// delete single element identified by index if index column
				if (propIndColumnInfo) {

					// index column expression for SQL
					const indColExpr = propIndColumnInfo.tableAlias + '.' +
						propIndColumnInfo.columnName;

					// delete the element identified by index
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias,
						`${indColExpr} = ${ptr.collectionElementIndex}`));

					// shift element indexes left
					this._commands.push(new UpdateColumnCommand(
						propCtx, propIndColumnInfo, `${indColExpr} - 1`,
						`${indColExpr} > ${ptr.collectionElementIndex}`));

				} else if (propDesc.allowDuplicates) { // re-populate if dupes

					// clear existing array
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias));

					// re-populate new array
					this._commands.push(new PopulateSimpleArrayCommand(
						propDesc.table, propDesc.parentIdColumn,
						this._dbDriver.sql(propCtx.parentIdValue),
						null, null,
						propValColumnInfo.columnName,
						propCtx.fullArray.map(v => this._valueToSql(propDesc, v))
					));

				} else { // unique values, no index column

					// delete the element identified by value
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias,
						propValColumnInfo.tableAlias + '.' +
							propValColumnInfo.columnName + ' = ' +
							this._valueToSql(propDesc, oldValue)));
				}
			}

		} else { // map element

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing object
				const pidColumnInfo = this._translationCtx.getPropValueColumn(
					ptr.propPath + '.$parentId');
				this._commands.push(new ClearObjectsCommand(
					propCtx, pidColumnInfo.tableAlias));

			} else { // simple value

				// delete element
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				this._commands.push(new ClearSimpleCollectionCommand(
					propCtx, propValColumnInfo.tableAlias));
			}
		}
	}

	// process update
	onSet(op, ptr, newValue, oldValue) {

		// get property context
		const propDesc = ptr.propDesc;
		const propCtx = this._getPropertyContext(ptr, (
			(propDesc.scalarValueType === 'object') && propDesc.table ?
				oldValue : undefined));

		// update entanglements
		if (propDesc.isEntangled()) {
			const entangledIds = this._getEntangledRecordIds(propDesc);
			const targetRecordTypeDesc = propDesc.nestedProperties;
			if (propDesc.isArray() && !ptr.collectionElement) {
				if (Array.isArray(oldValue))
					for (let ref of oldValue)
						if ((typeof ref) === 'string')
							entangledIds.push(targetRecordTypeDesc.refToId(ref));
				if (Array.isArray(newValue))
					for (let ref of newValue)
						if ((typeof ref) === 'string')
							entangledIds.push(targetRecordTypeDesc.refToId(ref));
			} else if (propDesc.isMap() && !ptr.collectionElement) {
				if (((typeof oldValue) === 'object') && (oldValue !== null))
					for (let key in oldValue) {
						const ref = oldValue[key];
						if ((typeof ref) === 'string')
							entangledIds.push(targetRecordTypeDesc.refToId(ref));
					}
				if (((typeof newValue) === 'object') && (newValue !== null))
					for (let key in newValue) {
						const ref = newValue[key];
						if ((typeof ref) === 'string')
							entangledIds.push(targetRecordTypeDesc.refToId(ref));
					}
			} else {
				if ((typeof oldValue) === 'string')
					entangledIds.push(targetRecordTypeDesc.refToId(oldValue));
				if ((typeof newValue) === 'string')
					entangledIds.push(targetRecordTypeDesc.refToId(newValue));
			}
		}

		// create the commands depending on the target type
		if (!ptr.collectionElement && propDesc.isArray()) { // whole array

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing objects if any
				const pidColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$parentId');
				if (oldValue && (oldValue.length > 0))
					this._commands.push(new ClearObjectsCommand(
						propCtx, pidColumnInfo.tableAlias));

				// insert new objects if any
				if (newValue && (newValue.length > 0)) {
					this.addGeneratedParam(
						propCtx.parentIdPropPath, propCtx.parentIdValue);
					for (let i = 0, len = newValue.length; i < len; i++)
						this._dbo._createInsertCommands(
							this._commands, propDesc.table,
							propDesc.parentIdColumn, propCtx.parentIdPropPath,
							propDesc, i, propDesc.nestedProperties, newValue[i]);
				}

			} else { // simple value

				// clear existing array if not empty
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				if (oldValue && (oldValue.length > 0))
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias));

				// populate new array if not empty
				if (newValue && (newValue.length > 0))
					this._commands.push(new PopulateSimpleArrayCommand(
						propDesc.table, propDesc.parentIdColumn,
						this._dbDriver.sql(propCtx.parentIdValue),
						propDesc.indexColumn, 0,
						propValColumnInfo.columnName,
						newValue.map(v => this._valueToSql(propDesc, v))
					));
			}

		} else if (!ptr.collectionElement && propDesc.isMap()) { // whole map

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing objects if any
				const pidColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$parentId');
				if (oldValue && (Object.keys(oldValue).length > 0))
					this._commands.push(new ClearObjectsCommand(
						propCtx, pidColumnInfo.tableAlias));

				// insert new objects if any
				const newKeys = (newValue && Object.keys(newValue));
				if (newKeys && (newKeys.length > 0)) {
					this.addGeneratedParam(
						propCtx.parentIdPropPath, propCtx.parentIdValue);
					for (let key of newKeys)
						this._dbo._createInsertCommands(
							this._commands, propDesc.table,
							propDesc.parentIdColumn, propCtx.parentIdPropPath,
							propDesc, key,
							propDesc.nestedProperties, newValue[key]);
				}

			} else { // simple value

				// clear existing map if not empty
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				if (oldValue && (Object.keys(oldValue).length > 0))
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias));

				// populate new map if not empty
				const newKeys = (newValue && Object.keys(newValue));
				if (newKeys && (newKeys.length > 0)) {
					const propKeyColumnInfo =
						this._translationCtx.getPropValueColumn(
							ptr.propPath + '.$key');
					this._commands.push(new PopulateSimpleMapCommand(
						propDesc.table, propDesc.parentIdColumn,
						this._dbDriver.sql(propCtx.parentIdValue),
						propKeyColumnInfo.columnName,
						propValColumnInfo.columnName,
						newKeys.map(k => [
							this._dbo._makeMapKeySql(propDesc, k),
							this._valueToSql(propDesc, newValue[k])
						])
					));
				}
			}

		} else if (propDesc.isArray()) { // array element

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing object
				const pidColumnInfo = this._translationCtx.getPropValueColumn(
					ptr.propPath + '.$parentId');
				this._commands.push(new ClearObjectsCommand(
					propCtx, pidColumnInfo.tableAlias));

				// insert new object
				this.addGeneratedParam(
					propCtx.parentIdPropPath, propCtx.parentIdValue);
				this._dbo._createInsertCommands(
					this._commands, propDesc.table,
					propDesc.parentIdColumn, propCtx.parentIdPropPath,
					propDesc, ptr.collectionElementIndex,
					propDesc.nestedProperties, newValue);

			} else { // simple value

				// get value and element index columns infos
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				const propIndColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$index');

				// update single element identified by index if index column
				if (propIndColumnInfo) {

					// index column expression for SQL
					const indColExpr = propIndColumnInfo.tableAlias + '.' +
						propIndColumnInfo.columnName;

					// update the element identified by index
					this._commands.push(new UpdateColumnCommand(
						propCtx, propValColumnInfo,
						this._valueToSql(propDesc, newValue),
						`${indColExpr} = ${ptr.collectionElementIndex}`));

				} else if (propDesc.allowDuplicates) { // re-populate if dupes

					// clear existing array
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias));

					// re-populate new array
					this._commands.push(new PopulateSimpleArrayCommand(
						propDesc.table, propDesc.parentIdColumn,
						this._dbDriver.sql(propCtx.parentIdValue),
						null, null,
						propValColumnInfo.columnName,
						propCtx.fullArray.map(v => this._valueToSql(propDesc, v))
					));

				} else { // unique values, no index column

					// update the element identified by value
					this._commands.push(new UpdateColumnCommand(
						propCtx, propValColumnInfo,
						this._valueToSql(propDesc, newValue),
						propValColumnInfo.tableAlias + '.' +
							propValColumnInfo.columnName + ' = ' +
							this._valueToSql(propDesc, oldValue)));
				}
			}

		} else if (propDesc.isMap()) { // map element

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// delete existing object
				const pidColumnInfo = this._translationCtx.getPropValueColumn(
					ptr.propPath + '.$parentId');
				this._commands.push(new ClearObjectsCommand(
					propCtx, pidColumnInfo.tableAlias));

				// insert new object
				this.addGeneratedParam(
					propCtx.parentIdPropPath, propCtx.parentIdValue);
				this._dbo._createInsertCommands(
					this._commands, propDesc.table,
					propDesc.parentIdColumn, propCtx.parentIdPropPath,
					propDesc, ptr.collectionElementIndex,
					propDesc.nestedProperties, newValue);

			} else { // simple value

				// replace existing value
				const propValColumnInfo =
					this._translationCtx.getPropValueColumn(
						ptr.propPath + '.$value');
				this._commands.push(new UpdateColumnCommand(
					propCtx, propValColumnInfo,
					this._valueToSql(propDesc, newValue)));
			}

		} else { // scalar

			// object or simple?
			if (propDesc.scalarValueType === 'object') {

				// stored in its own table?
				if (propDesc.table) {

					// delete existing object if any
					const pidColumnInfo =
						this._translationCtx.getPropValueColumn(
							ptr.propPath + '.$parentId');
					if (oldValue)
						this._commands.push(new ClearObjectsCommand(
							propCtx, pidColumnInfo.tableAlias));

					// insert new object if any
					if (newValue) {
						this.addGeneratedParam(
							propCtx.parentIdPropPath, propCtx.parentIdValue);
						this._dbo._createInsertCommands(
							this._commands, propDesc.table,
							propDesc.parentIdColumn, propCtx.parentIdPropPath,
							null, null, propDesc.nestedProperties, newValue);
					}

				} else { // same table

					// go over every stored property
					const container = propDesc.nestedProperties;
					for (let childPropName of container.allPropertyNames) {
						const childPropDesc = container.getPropertyDesc(
							childPropName);
						if (childPropDesc.isCalculated() ||
							childPropDesc.isView())
							continue;
						this.onSet(
							op, ptr.createChildPointer(childPropName),
							(newValue && newValue[childPropName]),
							(oldValue && oldValue[childPropName]));
					}
				}

			} else { // simple value

				// replace existing value
				this._commands.push(new UpdateColumnCommand(
					propCtx,
					this._translationCtx.getPropValueColumn(ptr.propPath),
					this._valueToSql(propDesc, newValue)
				));
			}
		}
	}

	/**
	 * Convert property value to SQL value expression.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Property
	 * descriptor.
	 * @param {*} val Property value.
	 * @returns {string} Value SQL.
	 */
	_valueToSql(propDesc, val) {

		if (propDesc.isRef() && ((typeof val) === 'string'))
			return this._dbDriver.sql(propDesc.nestedProperties.refToId(val));

		return this._dbDriver.sql(val);
	}

	/**
	 * Get entangled record ids list for the specified reference property. The
	 * returned list is for the current context record only.
	 *
	 * @private
	 * @param {module:x2node-records~PropertyDescrpitor} propDesc Entangled
	 * reference property descrpitor.
	 * @returns {Array.<(string|number)>} The entangled record ids list.
	 */
	_getEntangledRecordIds(propDesc) {

		if (!this._recordEntangledUpdates)
			this._recordEntangledUpdates = new Object();

		let ids = this._recordEntangledUpdates[propDesc.refTarget];
		if (!ids)
			this._recordEntangledUpdates[propDesc.refTarget] = ids = new Array();

		return ids;
	}

	/**
	 * Get context of the property pointed by the specified pointer. The context
	 * links the property to its parents and helps perform modification
	 * operations on it.
	 *
	 * @private
	 * @param {module:x2node-pointers~RecordElementPointer} ptr Property pointer.
	 * @param {*} [removedElementValue] If called for removed collection element,
	 * this is the removed element's value.
	 * @returns {Object} The property context object.
	 */
	_getPropertyContext(ptr, removedElementValue) {

		// initial property context object
		const propCtx = {

			anchors: new Array(),
			_uniqueIdPropPath: undefined,

			_containerDesc: undefined,
			_containerObj: undefined,

			fullArray: undefined,

			get parentIdPropPath() {
				return this._containerDesc.nestedPath +
					this._containerDesc.idPropertyName;
			},

			get parentIdValue() {
				return this._containerObj[this._containerDesc.idPropertyName];
			},

			get anchorsExpr() {
				return (this._anchorsExpr || (
					this._anchorsExpr = this.anchors.map(
						a => a.columnExpr + ' = ' + a.valueExpr).join(' AND ')));
			}
		};

		// trace the pointer through the record and extract relevant context data
		ptr.getValue(this._record, (prefixPtr, value, prefixDepth) => {

			// initialize context with root data
			if (prefixPtr.isRoot()) {

				propCtx.anchors.push({
					columnExpr: this._translationCtx.translatePropPath(
						this._recordIdPropName),
					valueExpr: this._dbDriver.sql(value[this._recordIdPropName])
				});

				if (!propCtx._containerDesc) {
					propCtx._containerDesc = this._recordTypeDesc;
					propCtx._containerObj = this._record;
				}

				propCtx._uniqueIdPropPath = this._recordIdPropName;

			} else { // not root

				// get intermediate/leaf property descriptor
				const propDesc = prefixPtr.propDesc;

				// add anchor if collection element
				let idAnchorAdded = false;
				if (prefixPtr.collectionElement) {
					if (propDesc.isArray()) {
						if ((propDesc.scalarValueType === 'object') &&
							(prefixPtr.collectionElementIndex !== '-')) {
							const idPropName =
								propDesc.nestedProperties.idPropertyName;
							const elementObj = (
								prefixDepth > 0 ? value :
									(removedElementValue || value));
							propCtx.anchors.push({
								columnExpr:
								this._translationCtx.translatePropPath(
									prefixPtr.propPath + '.' + idPropName),
								valueExpr:
								this._dbDriver.sql(elementObj[idPropName])
							});
							idAnchorAdded = true;
						}
					} else { // it's a map
						propCtx.anchors.push({
							columnExpr: this._translationCtx.translatePropPath(
								prefixPtr.propPath + '.$key'),
							valueExpr: this._dbo._makeMapKeySql(
								prefixPtr.propDesc,
								prefixPtr.collectionElementIndex)
						});
					}

				}

				// save reference to the full array
				else if (propDesc.isArray() && !propCtx.fullArray) {
					propCtx.fullArray = value;
				}

				// check if object with id
				const objectWithId = (
					(propDesc.scalarValueType === 'object') &&
						propDesc.nestedProperties.idPropertyName);

				// save immediate container
				if ((prefixDepth > 0) && objectWithId &&
					!propCtx._containerDesc) {
					propCtx._containerDesc = propDesc.nestedProperties;
					propCtx._containerObj = value;
				}

				// chop the anchors if unique id table
				if (objectWithId) {
					const nestedProps = propDesc.nestedProperties;
					const idPropName = nestedProps.idPropertyName;
					if ((propDesc.isScalar() || (
						prefixPtr.collectionElement &&
							(prefixPtr.collectionElementIndex !== '-'))) &&
						nestedProps.getPropertyDesc(idPropName).tableUnique) {
						propCtx._uniqueIdPropPath =
							prefixPtr.propPath + '.' + idPropName;
						if (idAnchorAdded) {
							propCtx.anchors.splice(
								0, propCtx.anchors.length - 1);
						} else {
							propCtx.anchors.length = 0;
							const elementObj = (
								prefixDepth > 0 ? value :
									(removedElementValue || value));
							propCtx.anchors.push({
								columnExpr:
								this._translationCtx.translatePropPath(
									propCtx._uniqueIdPropPath),
								valueExpr:
								this._dbDriver.sql(elementObj[idPropName])
							});
						}
					}
				}
			}
		});

		// get unique id table id column info
		propCtx.uniqueIdColumnInfo =
			this._translationCtx.getPropValueColumn(propCtx._uniqueIdPropPath);

		// return the property context
		return propCtx;
	}

	// process test
	onTest(ptr, value, passed) {

		if (!passed)
			this._recordTestFailed = true;
	}
}


/////////////////////////////////////////////////////////////////////////////////
// THE DBO
/////////////////////////////////////////////////////////////////////////////////

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
	 * @param {module:x2node-dbos.RecordCollectionsMonitor} rcMonitor The record
	 * collections monitor.
	 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc The
	 * record type descriptor.
	 * @param {module:x2node-patches~RecordPatch} patch The patch.
	 * @param {Array.<Array>} [filterSpec] Optional filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(
		dbDriver, recordTypes, rcMonitor, recordTypeDesc, patch, filterSpec) {
		super(dbDriver, recordTypes, rcMonitor);

		// save the record type descriptor (used by the execution context)
		this._recordTypeDesc = recordTypeDesc;

		// the operation commands sequence
		this._commands = new Array();

		// add the records pre-fetch command
		this._commands.push(new PrefetchCommand(new FetchDBO(
			dbDriver, recordTypes, recordTypeDesc.name,
			[ '*' ], null, filterSpec, null, null, 'exclusive')));

		// add record updates command
		this._commands.push(new UpdateRecordsCommand(patch));

		// add entangled records update commands
		this._commands.push(this._createUpdateEntangledRecordsCommand());

		// add record collections monitor notification command
		this._commands.push(this._createNotifyRecordCollectionsMonitorCommand());

		// build update query tree:

		// add nested object ids to the update properties tree
		const involvedPropPaths = new Set(patch.involvedPropPaths);
		for (let propPath of involvedPropPaths) {
			let container = recordTypeDesc;
			for (let propName of propPath.split('.')) {
				const propDesc = container.getPropertyDesc(propName);
				container = propDesc.nestedProperties;
				if (container && container.idPropertyName)
					involvedPropPaths.add(
						container.nestedPath + container.idPropertyName);
			}
		}

		// add record meta-info props to the update properties tree
		[ 'version', 'modificationTimestamp', 'modificationActor' ]
			.forEach(r => {
				const propName = recordTypeDesc.getRecordMetaInfoPropName(r);
				if (propName) {
					involvedPropPaths.add(propName);
					if (r === 'modificationActor')
						this._actorRequired = true;
				}
			});

		// build update properties tree
		const baseValueExprCtx = new ValueExpressionContext(
			'', [ recordTypeDesc ]);
		const recordsPropDesc = recordTypes.getRecordTypeDesc(
			recordTypeDesc.superRecordTypeName).getPropertyDesc('records');
		const updatePropsTree = propsTreeBuilder.buildSimplePropsTree(
			recordTypes, recordsPropDesc, 'update', baseValueExprCtx,
			involvedPropPaths);

		// build update query tree (used by the execution context)
		this._updateQueryTree = queryTreeBuilder.forDirectQuery(
			dbDriver, recordTypes, 'update', false, updatePropsTree);
	}

	/**
	 * Validates the record after applying the patch but before saving it to the
	 * database.
	 *
	 * @callback module:x2node-dbos~UpdateDBO~recordValidator
	 * @param {Object} record The record after the patch has been applied. The
	 * record includes all properties that are fetched by default (by default,
	 * that includes all properties that are not views, not calculated and not
	 * dependent record references).
	 * @returns {(*|Promise)} If returns a promise, it can be either resolved to
	 * proceed with the record save or rejected, in which case the whole DBO
	 * execution is aborted and the promise returned by the DBO's
	 * [execute()]{@link module:x2node-dbos~UpdateDBO~execute} method is rejected
	 * with the record validator's rejection object. If returned value is not a
	 * promise (including nothing), the record save continues.
	 */

	/**
	 * Record validator functions.
	 *
	 * @typedef {Object} module:x2node-dbos~UpdateDBO~RecordValidators
	 * @property {module:x2node-dbos~UpdateDBO~recordValidator} [beforePatch]
	 * Validator function invoked before applying the patch.
	 * @property {module:x2node-dbos~UpdateDBO~recordValidator} [afterPatch]
	 * Validator function invoked after applying the patch.
	 */

	/**
	 * Update DBO execution result object.
	 *
	 * @typedef module:x2node-dbos~UpdateDBO~Result
	 * @property {Array.<Object>} records All patched matched records with all
	 * properties that are fetched by default.
	 * @property {Array.<(string|number)>} updatedRecordIds Ids of records
	 * actually modified by the operation, empty array if none.
	 * @property {boolean} testFailed <code>true</code> if any of the matched
	 * records were not updated because a "test" patch operation failed.
	 * @property {Array} [failedRecordIds] If <code>testFailed</code> is
	 * <code>true</code>, this lists the ids of the failed records.
	 */

	/**
	 * Execute the operation.
	 *
	 * @param {(module:x2node-dbos~Transaction|*)} txOrCon The active database
	 * transaction, or database connection object compatible with the database
	 * driver to have the method automatically organize the transaction around
	 * the operation execution.
	 * @param {?module:x2node-common.Actor} actor Actor executing the DBO.
	 * @param {?(module:x2node-dbos~UpdateDBO~RecordValidators|module:x2node-dbos~UpdateDBO~recordValidator)} recordValidators
	 * Record validation/normalization functions, or <code>null</code> if none If
	 * function is provided, it is assumed as "afterPatch" function.
	 * @param {Object.<string,*>} [filterParams] Filter parameters. The keys are
	 * parameter names, the values are parameter values. Note, that no value type
	 * conversion is performed: strings are used as SQL strings, numbers as SQL
	 * numbers, etc. Arrays are expanded into comma-separated lists of element
	 * values. Functions are called without arguments and the results are used as
	 * values. Otherwise, values can be strings, numbers, Booleans and nulls.
	 * @returns {Promise.<module:x2node-dbos~UpdateDBO~Result>} The operation
	 * result object promise. The promise is rejected with the error object if an
	 * error happens during the operation execution (failed "test" operation is
	 * not considered an error). Also may be rejected with the provided
	 * <code>recordValidator</code> function rejection result.
	 * @throws {module:x2node-common.X2UsageError} If provided filter
	 * parameters object is invalid (missing parameter, <code>NaN</code> value or
	 * value of unsupported type).
	 */
	execute(txOrCon, actor, recordValidators, filterParams) {

		return this._executeCommands(new UpdateDBOExecutionContext(
			this, txOrCon, actor, filterParams, recordValidators));
	}
}

// export the class
module.exports = UpdateDBO;
