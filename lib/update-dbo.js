'use strict';

const common = require('x2node-common');
const pointers = require('x2node-pointers');

const AbstractDBO = require('./abstract-dbo.js');
const DBOExecutionContext = require('./dbo-execution-context.js');
const FetchDBO = require('./fetch-dbo.js');
const ValueExpressionContext = require('./value-expression-context.js');
const propsTreeBuilder = require('./props-tree-builder.js');
const queryTreeBuilder = require('./query-tree-builder.js');


// TODO: updating linked references table may update ref target record meta

/**
 * Abstract operation sequence command.
 *
 * @private
 * @memberof module:x2node-dbos
 * @inner
 * @implements module:x2node-dbos.DBOCommand
 * @abstract
 */
class UpdateDBOCommand {

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
 * @extends module:x2node-dbos~UpdateDBOCommand
 */
class UpdateColumnCommand extends UpdateDBOCommand {

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
	execute(promiseChain, ctx) {

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
 * @extends module:x2node-dbos~UpdateDBOCommand
 */
class ClearSimpleCollectionCommand extends UpdateDBOCommand {

	constructor(propCtx, tableAlias, addlCondExpr) {
		super(propCtx);

		this._tableAlias = tableAlias;
		this._addlCondExpr = addlCondExpr;
	}

	// add command execution to the chain
	execute(promiseChain, ctx) {

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
 * @extends module:x2node-dbos~UpdateDBOCommand
 */
class PopulateSimpleArrayCommand extends UpdateDBOCommand {

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
	execute(promiseChain, ctx) {

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
 * @extends module:x2node-dbos~UpdateDBOCommand
 */
class PopulateSimpleMapCommand extends UpdateDBOCommand {

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
	execute(promiseChain, ctx) {

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
 * @extends module:x2node-dbos~UpdateDBOCommand
 */
class ClearObjectsCommand extends UpdateDBOCommand {

	constructor(propCtx, tableAlias) {
		super(propCtx);

		this._tableAlias = tableAlias;
	}

	// add command execution to the chain
	execute(promiseChain, ctx) {

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

	constructor(dbo, txOrCon, actor, filterParams) {
		super(dbo, txOrCon, actor, filterParams);

		// operation specific info
		this._updateQueryTree = dbo._updateQueryTree;
		this._translationCtx = this._updateQueryTree.getTopTranslationContext(
			dbo._paramsHandler);
		this._recordTypeDesc = dbo._recordTypeDesc;
		this._recordIdPropName = dbo._recordTypeDesc.idPropertyName;

		// whole operation result
		this._records = new Array();
		this._updatedRecordIds = new Array();
		this._testFailed = false;
		this._failedRecordIds = undefined;

		// entangled updates
		this._entangledUpdates = new Object();

		// context record data
		this._record = null;
		this._commands = new Array();
		this._recordTestFailed = null;
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


	// execution cycle methods:

	/**
	 * Start processing of a record update.
	 *
	 * @param {Object} record The record data read from the database for update.
	 */
	startRecord(record) {

		// save the record in the context
		this._records.push(record);
		this._record = record;

		// reset the context record commands
		this._commands.length = 0;
		this._recordTestFailed = false;
	}

	/**
	 * Flush current record updates.
	 *
	 * @param {Promise} promiseChain The promise chain.
	 * @returns {Promise} The promise chain with record update operations added.
	 */
	flushRecord(promiseChain) {

		// check if "test" operation failed for the current record
		if (this._recordTestFailed) {
			this._testFailed = true;
			if (!this._failedRecordIds)
				this._failedRecordIds = new Array();
			this._failedRecordIds.push(this._record[this._recordIdPropName]);
			return promiseChain;
		}

		// check if the current record needs any modification
		if (this._commands.length === 0)
			return promiseChain;

		// the record was modified, save the modifications into the database:

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
			// HINT: should be locked, or TODO: add condition for the version
			this._commands.unshift(new UpdateColumnCommand(
				rootPropCtx,
				this._translationCtx.getPropValueColumn(metaPropName),
				//this._translationCtx.translatePropPath(metaPropName) + ' + 1'
				this._dbDriver.sql(++this._record[metaPropName])
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
		let resPromise = promiseChain;

		// queue up the commands
		for (let cmd of this._commands)
			resPromise = cmd.execute(resPromise, this);

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

		// create the commands depending on whether it's an array or map
		const propDesc = ptr.propDesc;
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
					[ this._dbDriver.sql(newValue) ]
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
						this._dbDriver.sql(newValue)
					]]
				));
			}
		}
	}

	// process array/map element removal
	onRemove(op, ptr, oldValue) {

		// get property context
		const propCtx = this._getPropertyContext(ptr, oldValue);

		// create the commands depending on whether it's an array or map
		const propDesc = ptr.propDesc;
		if (propDesc.isArray()) {

			// get element index column info
			const propIndColumnInfo =
				this._translationCtx.getPropValueColumn(ptr.propPath + '.$index');

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
						propCtx.fullArray.map(v => this._dbDriver.sql(v))
					));

				} else { // unique values, no index column

					// delete the element identified by value
					this._commands.push(new ClearSimpleCollectionCommand(
						propCtx, propValColumnInfo.tableAlias,
						propValColumnInfo.tableAlias + '.' +
							propValColumnInfo.columnName + ' = ' +
							this._dbDriver.sql(oldValue)));
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
						newValue.map(v => this._dbDriver.sql(v))
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
							this._dbDriver.sql(newValue[k])
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
						propCtx, propValColumnInfo, this._dbDriver.sql(newValue),
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
						propCtx.fullArray.map(v => this._dbDriver.sql(v))
					));

				} else { // unique values, no index column

					// update the element identified by value
					this._commands.push(new UpdateColumnCommand(
						propCtx, propValColumnInfo, this._dbDriver.sql(newValue),
						propValColumnInfo.tableAlias + '.' +
							propValColumnInfo.columnName + ' = ' +
							this._dbDriver.sql(oldValue)));
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
					propCtx, propValColumnInfo, this._dbDriver.sql(newValue)));
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
					for (let childPropName of propDesc.allPropertyNames) {
						const childPropDesc =
							propDesc.nestedProperties.getPropertyDesc(
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
					this._dbDriver.sql(newValue)
				));
			}
		}
	}

	_registerEntangledUpdate(propDesc, referredId) {

		if (propDesc.isEntangled()) {
			let ids = this._entangledUpdates[propDesc.refTarget];
			if (!ids)
				this._entangledUpdates[propDesc.refTarget] = ids = new Array();
			ids.push(referredId);
		}
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
	 * @param {module:x2node-patches~RecordPatch} patch The patch.
	 * @param {Array.<Array>} [filterSpec] Optional filter specification.
	 * @throws {module:x2node-common.X2UsageError} If the provided data is
	 * invalid.
	 */
	constructor(dbDriver, recordTypes, recordTypeDesc, patch, filterSpec) {
		super(dbDriver, recordTypes);

		// save the basics
		this._recordTypeDesc = recordTypeDesc;
		this._patch = patch;

		// assume actor not required until appropriate meta-info prop detected
		this._actorRequired = false;

		/* RELY ON fetchByDefault
		// collect record properties to load before patch
		const involvedPropPaths = new Array();
		this._collectAllStoredProps(recordTypeDesc, involvedPropPaths);
		*/

		// build the initial fetch DBO
		this._fetchDBO = new FetchDBO(
			dbDriver, recordTypes, recordTypeDesc.name, [ '*' ], [], filterSpec);

		// add nested object ids to the update properties tree
		const involvedPropPaths = new Set(this._patch.involvedPropPaths);
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

		// build update query tree
		this._updateQueryTree = queryTreeBuilder.forDirectQuery(
			dbDriver, recordTypes, 'update', false, updatePropsTree);
	}

	/**
	 * Recursively collect path of all stored properties.
	 *
	 * @private
	 * @param {module:x2node-records~PropertiesContainer} container Container
	 * descriptor.
	 * @param {Array.<string>} propPaths Array, to which to add property paths.
	 */
	/*_collectAllStoredProps(container, propPaths) {

		// go over the container properties
		for (let propName of container.allPropertyNames) {
			const propDesc = container.getPropertyDesc(propName);

			// skip non-stored properties
			if (propDesc.isView() || propDesc.isCalculated() ||
				propDesc.reverseRefPropertyName)
				continue;

			// check if nested object
			if (propDesc.scalarValueType === 'object')
				this._collectAllStoredProperties(
					propDesc.nestedProperties, propPaths);
			else // add the property
				propPaths.push(container.nestedPath + propName);
		}
	}*/

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
	 * @param {?module:x2node-dbos~UpdateDBO~recordValidator} recordValidator
	 * Record validation/normalization function, or <code>null</code> if none.
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
	execute(txOrCon, actor, recordValidator, filterParams) {

		// check if actor is required
		if (this._actorRequired && !actor)
			throw new common.X2UsageError('Operation may not be anonymous.');

		// create operation execution context
		const ctx = new UpdateDBOExecutionContext(
			this, txOrCon, actor, filterParams);

		// initial pre-resolved result promise for the chain
		let resPromise = Promise.resolve();

		// start transaction if necessary
		if (ctx.wrapInTx)
			resPromise = this._startTx(resPromise, ctx);

		// queue up initial fetch
		// TODO: lock records for update
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
					if (recordValidator) {
						recordsChain = recordsChain.then(
							() => recordValidator(record),
							err => Promise.reject(err)
						);
					}
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
