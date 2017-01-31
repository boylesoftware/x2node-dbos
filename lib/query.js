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
	 * @param {module:x2node-queries~QuerySpec} querySpec Query specification.
	 */
	constructor(dbDriver, querySpec) {

		this._dbDriver = dbDriver;
		this._querySpec = querySpec;

		//...

		this._withSuperaggregates = false;

		const queryBuilders = new Array();
		this._assembleQueryBuilders(
			queryBuilders, true, this._buildQueryTree(), false);
		this._queries = queryBuilders.map(queryBuilder => queryBuilder.toSQL());
	}

	/**
	 * Build query tree.
	 *
	 * @private
	 * TODO: write jsdoc tags
	 */
	_buildQueryTree() {

		// create root node
		const rootNode = {

			createChildNode(
				table, many, virtual, joinByColumn, joinToColumn, keyColumn) {

				const node = {
					_nextTableAliasDisc: 'a'.charCodeAt(0),
					parent: (this === rootNode ? null : this),
					table: table,
					tableAlias: (
						this.tableAlias ?
							this.tableAlias + String.fromCharCode(
								this._nextTableAliasDisc++) : 'z'),
					many: many,
					virtual: virtual,
					joinByColumn: joinByColumn,
					joinToColumn: joinToColumn,
					keyColumn: keyColumn,
					keys: [],
					props: [],
					children: []
				};
				node.createChildNode = this.createChildNode;

				if (this.children)
					this.children.push(node);

				return node;
			}
		};

		// add top records collection to the root node
		const recordsProp = this._querySpec.selectedProps.get('records');
		const recordsPropDesc = recordsProp.desc;
		const recordsPropDef = recordsPropDesc.definition;
		if (!recordsPropDef.table)
			throw new common.X2UsageError(
				'Specified top record type ' +
					this._querySpec.topRecordTypeName +
					' does not have database table associated with it.');
		const topIdPropName = recordsPropDesc.nestedProperties.idPropertyName;
		const topIdColumn = recordsPropDesc.nestedProperties.getPropertyDesc(
			topIdPropName).definition.column;
		if (!topIdColumn)
			throw new common.X2UsageError(
				'Id property of the specified top record type ' +
					this._querySpec.topRecordTypeName +
					' does not have a database table column associated with' +
					' it.');
		const queryTree = rootNode.createChildNode(
			recordsPropDef.table, true, false, null, null, topIdColumn);

		// add top record id property to have it in front of the select list
		const topIdProp = {
			expr: queryTree.tableAlias + '.' + topIdColumn,
			markup: topIdPropName
		};
		queryTree.keys.push(topIdProp);
		queryTree.props.push(topIdProp);

		// add the rest of selected child properties
		const topMarkupCtx = {
			prefix: '',
			nextChildMarkupDisc: 'a'.charCodeAt(0)
		};
		for (let p of recordsProp.children.values())
			if (!p.desc.isId()) // already included
				this._addSelectedPropToQueryTree(p, queryTree, topMarkupCtx);

		// return the query tree
		return queryTree;
	}

	/**
	 * Add selected property node to the query tree node.
	 *
	 * @private
	 * TODO: write jsdoc tags
	 */
	_addSelectedPropToQueryTree(selectedProp, parentQueryTreeNode, markupCtx) {

		const recordTypes = this._querySpec.recordTypes;

		// record definition helper functions
		function isVirtual(propDef, virtualByDefault) {
			return (
				propDef.optional === undefined ?
					virtualByDefault : propDef.optional
			);
		}
		function mustHaveParentIdColumn(propDesc) {
			if (!propDesc.definition.parentIdColumn)
				throw new common.X2UsageError(
					'Property ' + propDesc.container.nestedPath + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						' must have a parent id column in its definition.');
		}
		function mustHaveTable(propDesc) {
			if (!propDesc.definition.table)
				throw new common.X2UsageError(
					'Property ' + propDesc.container.nestedPath + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						' is not scalar and must be stored in a separate' +
						' database table.');
			mustHaveParentIdColumn(propDesc);
		}
		function mustHaveColumn(propDesc) {
			if (!propDesc.definition.column)
				throw new common.X2UsageError(
					'Property ' + propDesc.container.nestedPath +
						propDesc.name + ' of record type ' +
						propDesc.container.recordTypeName +
						' does not have a database table column associated' +
						' with it.');
		}
		function mustHaveKeyColumn(propDesc) {
			if (!propDesc.definition.keyColumn)
				throw new common.X2UsageError(
					'Property ' + propDesc.container.nestedPath +
						propDesc.name + ' of record type ' +
						propDesc.container.recordTypeName +
						' does not have a map key database table column' +
						' associated with it.');
		}
		function getIdColumn(container) {
			const idColumn = container.getPropertyDesc(
				container.idPropertyName).definition.column;
			if (!idColumn)
				throw new common.X2UsageError(
					'Property ' + container.nestedPath +
						container.idPropertyName + ' of record type ' +
						container.recordTypeName + ' does not have a database' +
						' table column associated with it.');
			return idColumn;
		}
		function getKeyColumn(propDesc, keyPropContainer) {
			const propDef = propDesc.definition;
			if (propDef.keyColumn)
				return propDef.keyColumn;
			if (!propDesc.keyPropertyName)
				throw new common.X2UsageError(
					'Property ' + propDesc.container.nestedPath +
						propDesc.name + ' of record type ' +
						propDesc.container.recordTypeName +
						' does not have a map key database table column' +
						' nor key property associated with it.');
			const keyPropDesc = keyPropContainer.getPropertyDesc(
				propDesc.keyPropertyName);
			if (!keyPropDesc.definition.column || keyPropDesc.definition.table)
				throw new common.X2UsageError(
					'Invalid definition of property ' +
						propDesc.container.nestedPath + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						': key property ' + keyPropDesc.name +
						' must be have a database table column associated with' +
						' it and must be stored in the same table as the' +
						' record.');
			return keyPropDesc.definition.column;
		}
		function getRefTargetDesc(propDesc) {
			const refTargetDesc = recordTypes.getRecordTypeDesc(
				propDesc.refTarget);
			if (!refTargetDesc.definition.table)
				throw new common.X2UsageError(
					'Record type ' + refTargetDesc.name +
						' does not have a database table associated with it' +
						' but is used as a target of reference property ' +
						propDesc.name + ' of ' +
						propDesc.container.recordTypeName + '.');
			return refTargetDesc;
		}
		function getReverseRefPropDesc(propDesc, refTargetDesc) {
			if (propDesc.container.nestedPath.length > 0)
				throw new common.X2UsageError(
					'Invalid definition of property ' +
						propDesc.container.nestedPath + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						': only top level record type reference property may' +
						' have reverse reference property in its definition.');
			const propDef = propDesc.definition;
			if (!refTargetDesc.hasProperty(propDef.reverseRefProperty))
				throw new common.X2UsageError(
					'Invalid definition of property ' + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						': reference target record type ' + propDesc.refTarget +
						' does not have reverse reference property ' +
						propDef.reverseRefProperty + '.');
			const reverseRefPropDesc = refTargetDesc.getPropertyDesc(
				propDef.reverseRefProperty);
			const myRecordTypeName = propDesc.container.recordTypeName;
			if (!reverseRefPropDesc.isRef() ||
				!reverseRefPropDesc.isScalar() ||
				reverseRefPropDesc.isPolymorph() ||
				(reverseRefPropDesc.refTarget !== myRecordTypeName))
				throw new common.X2UsageError(
					'Invalid definition of property ' + propDesc.name +
						' of record type ' + propDesc.container.recordTypeName +
						': the reverse reference property ' +
						propDef.reverseRefProperty + ' of ' +
						propDesc.refTarget + ' is not a scalar non-polymorphic' +
						' reference property pointing at ' +
						propDesc.container.recordTypeName + ' record type.');
			if (reverseRefPropDesc.definition.reverseRefProperty)
				throw new common.X2UsageError(
					'Property ' + propDef.reverseRefProperty +
						' of record type ' + propDesc.refTarget +
						' may not have reverse reference in its definition as' +
						' it is itself is used as a reverse reference for ' +
						' property ' + propDesc.name + ' of ' +
						propDesc.container.recordTypeName + '.');
			return reverseRefPropDesc;
		}

		// create markup prefix context for possible children
		const markupPrefix = markupCtx.prefix;
		const childrenMarkupCtx = (
			selectedProp.children && (selectedProp.children.size > 0) ? {
				prefix: markupPrefix.substring(0, markupPrefix.length - 1) +
					String.fromCharCode(markupCtx.nextChildMarkupDisc++) + '$',
				nextChildMarkupDisc: 'a'.charCodeAt(0)
			} :
			undefined
		);

		// process selected property node depending on its type
		const propDesc = selectedProp.desc;
		const propDef = propDesc.definition;
		let queryTreeNode;
		let fetch, keyColumn, anchor;
		let refTargetDesc, refTargetIdColumn;
		switch (
			(propDesc.isScalar() ? 'scalar' : (
				propDesc.isArray() ? 'array' : 'map')) +
				':' + propDesc.scalarValueType +
				':' + (propDesc.isPolymorph() ? 'poly' : 'mono')
		) {
		case 'scalar:string:mono':
		case 'scalar:number:mono':
		case 'scalar:boolean:mono':
		case 'scalar:datetime:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// validate definition
				mustHaveColumn(propDesc);
				mustHaveParentIdColumn(propDesc);

				// create child node
				queryTreeNode = parentQueryTreeNode.createChildNode(
					propDef.table, false, isVirtual(propDef, false),
					propDef.parentIdColumn, parentQueryTreeNode.keyColumn, null);

			} else { // stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// add value to the parent query tree node
				queryTreeNode = parentQueryTreeNode;
			}

			// add value
			queryTreeNode.props.push({
				expr: queryTreeNode.tableAlias + '.' + propDef.column,
				markup: markupPrefix + propDesc.name
			});

			break;

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// validate definition
				mustHaveParentIdColumn(propDesc);

				// create child node
				queryTreeNode = parentQueryTreeNode.createChildNode(
					propDef.table, false, isVirtual(propDef, false),
					propDef.parentIdColumn, parentQueryTreeNode.keyColumn,
					propDef.parentIdColumn);

				// create anchor
				anchor = {
					expr: queryTreeNode.tableAlias + '.' +
						queryTreeNode.keyColumn,
					markup: markupPrefix + propDesc.name
				};

			} else { // stored in the parent table

				// may not be optional
				if (propDef.optional)
					throw new common.X2UsageError(
						'Property ' + propDesc.container.nestedPath +
							propDesc.name + ' of record type ' +
							propDesc.container.recordTypeName +
							' may not be optional as it is stored in the' +
							' parent record table.');

				// create anchor
				anchor = {
					expr: this._dbDriver.booleanLiteral(true),
					markup: markupPrefix + propDesc.name
				};

				// add child properties to the parent node
				queryTreeNode = parentQueryTreeNode;
			}

			// add anchor
			queryTreeNode.keys.push(anchor);
			queryTreeNode.props.push(anchor);

			// add selected child properties
			for (let p of selectedProp.children.values())
				this._addSelectedPropToQueryTree(
					p, queryTreeNode, childrenMarkupCtx);

			break;

		case 'scalar:object:poly':

			// TODO:...

			break;

		case 'scalar:ref:mono':

			// get target record info
			refTargetDesc = getRefTargetDesc(propDesc);
			refTargetIdColumn = getIdColumn(refTargetDesc);

			// check if fetched
			fetch = (selectedProp.children && (selectedProp.children.size > 0));

			// check how it is stored
			if (propDef.reverseRefProperty) {

				// get reverse reference property descriptor
				const reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				const reverseRefPropDef = reverseRefPropDesc.definition;

				// check how the reverse reference property is stored
				if (reverseRefPropDef.table) {

					// validate definition
					mustHaveColumn(reverseRefPropDesc);
					mustHaveParentIdColumn(reverseRefPropDesc);

					// create child node
					queryTreeNode = parentQueryTreeNode.createChildNode(
						reverseRefPropDef.table, false,
						isVirtual(propDef, false),
						reverseRefPropDef.column, parentQueryTreeNode.keyColumn,
						reverseRefPropDef.parentIdColumn);

					// add value
					anchor = {
						expr: queryTreeNode.tableAlias + '.' +
							reverseRefPropDef.parentIdColumn,
						markup: markupPrefix + propDesc.name +
							(fetch ? ':' : '')
					};
					queryTreeNode.props.push(anchor);

					// create child node for the referred records if fetched
					if (fetch) {

						// create the node
						queryTreeNode = queryTreeNode.createChildNode(
							refTargetDesc.definition.table, false,
							queryTreeNode.virtual,
							refTargetIdColumn, queryTreeNode.keyColumn,
							refTargetIdColumn);

						// add anchor
						queryTreeNode.keys.push(anchor);
					}

				} else { // stored in the target record table

					// validate definition
					mustHaveColumn(reverseRefPropDesc);

					// create child node
					queryTreeNode = parentQueryTreeNode.createChildNode(
						refTargetDesc.definition.table, false,
						isVirtual(propDef, false),
						reverseRefPropDef.column, parentQueryTreeNode.keyColumn,
						refTargetIdColumn);

					// add value
					anchor = {
						expr: queryTreeNode.tableAlias + '.' + refTargetIdColumn,
						markup: markupPrefix + propDesc.name +
							(fetch ? ':' : '')
					};
					queryTreeNode.props.push(anchor);

					// add anchor
					if (fetch)
						queryTreeNode.keys.push(anchor);
				}

			} else if (propDef.table) {

				// validate definition
				mustHaveColumn(propDesc);
				mustHaveParentIdColumn(propDesc);

				// create child node
				queryTreeNode = parentQueryTreeNode.createChildNode(
					propDef.table, false, isVirtual(propDef, false),
					propDef.parentIdColumn, parentQueryTreeNode.keyColumn,
					propDef.column);

				// add value
				anchor = {
					expr: queryTreeNode.tableAlias + '.' + propDef.column,
					markup: markupPrefix + propDesc.name +
						(fetch ? ':' : '')
				};
				queryTreeNode.props.push(anchor);

				// create child node for the referred records if fetched
				if (fetch) {

					// create the node
					queryTreeNode = queryTreeNode.createChildNode(
						refTargetDesc.definition.table, false,
						queryTreeNode.virtual,
						refTargetIdColumn, queryTreeNode.keyColumn,
						refTargetIdColumn);

					// add anchor
					queryTreeNode.keys.push(anchor);
				}

			} else { // stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// add value
				anchor = {
					expr: parentQueryTreeNode.tableAlias + '.' + propDef.column,
					markup: markupPrefix + propDesc.name +
						(fetch ? ':' : '')
				};
				parentQueryTreeNode.props.push(anchor);

				// add child node if fetched
				if (fetch) {

					// create the node
					queryTreeNode = parentQueryTreeNode.createChildNode(
						refTargetDesc.definition.table, false,
						isVirtual(propDef, false),
						refTargetIdColumn, propDef.column,
						refTargetIdColumn);

					// add anchor
					queryTreeNode.keys.push(anchor);
				}
			}

			// add fetched target properties
			if (fetch)
				for (let p of selectedProp.children.values())
					this._addSelectedPropToQueryTree(
						p, queryTreeNode, childrenMarkupCtx);

			break;

		case 'scalar:ref:poly':

			// TODO:...

			break;

		case 'array:string:mono':
		case 'array:number:mono':
		case 'array:boolean:mono':
		case 'array:datetime:mono':
		case 'map:string:mono':
		case 'map:number:mono':
		case 'map:boolean:mono':
		case 'map:datetime:mono':

			// validate definition
			mustHaveTable(propDesc);
			mustHaveColumn(propDesc);
			if (propDesc.isMap())
				mustHaveKeyColumn(propDesc);

			// create child node
			queryTreeNode = parentQueryTreeNode.createChildNode(
				propDef.table, true, isVirtual(propDef, true),
				propDef.parentIdColumn, parentQueryTreeNode.keyColumn, null);

			// add anchor and value
			queryTreeNode.props.push({
				expr: queryTreeNode.tableAlias + '.' + (
					propDesc.isMap() ? propDef.keyColumn :
						propDef.parentIdColumn),
				markup: markupPrefix + propDesc.name
			});
			queryTreeNode.props.push({
				expr: queryTreeNode.tableAlias + '.' + propDef.column,
				markup: markupPrefix.substring(0, markupPrefix.length - 1) +
					String.fromCharCode(markupCtx.nextChildMarkupDisc++) + '$'
			});

			break;

		case 'array:object:mono':
		case 'map:object:mono':

			// validate definition
			mustHaveTable(propDesc);

			// create child node
			keyColumn = (
				propDesc.isMap() ?
					getKeyColumn(propDesc, propDesc.nestedProperties) :
					getIdColumn(propDesc.nestedProperties)
			);
			queryTreeNode = parentQueryTreeNode.createChildNode(
				propDef.table, true, isVirtual(propDef, true),
				propDef.parentIdColumn, parentQueryTreeNode.keyColumn,
				keyColumn);

			// add anchor
			anchor = {
				markup: markupPrefix + propDesc.name,
				expr: queryTreeNode.tableAlias + '.' + keyColumn
			};
			queryTreeNode.keys.push(anchor);
			queryTreeNode.props.push(anchor);

			// add selected child properties
			for (let p of selectedProp.children.values())
				this._addSelectedPropToQueryTree(
					p, queryTreeNode, childrenMarkupCtx);

			break;

		case 'array:object:poly':

			// TODO:...

			break;

		case 'array:ref:mono':

		case 'array:ref:poly':

		case 'map:object:poly':

		case 'map:ref:mono':

		case 'map:ref:poly':
		}
	}

	/**
	 * Recursively process the query sub-tree and create necessary query
	 * builders.
	 *
	 * @private
	 * TODO: write jsdoc tags
	 */
	_assembleQueryBuilders(
		queryBuilders, noExpansion, queryTreeNode, forceOuter) {

		// create branch if nessecary
		if (queryTreeNode.many && noExpansion)
			queryBuilders.push(this._createQueryBuilder(queryTreeNode));

		// get the current query builder
		let queryBuilder = queryBuilders[queryBuilders.length - 1];

		// add query tree node to the query builder
		this._addQueryTreeNodeToQueryBuilder(
			queryBuilder, queryTreeNode, forceOuter, false);

		// add query tree node's scalar children
		let noMoreChildExpansion = false;
		queryTreeNode.children.forEach(childQueryTreeNode => {
			if (!childQueryTreeNode.many)
				noMoreChildExpansion |= this._assembleQueryBuilders(
					queryBuilders, noMoreChildExpansion, childQueryTreeNode,
					(forceOuter || childQueryTreeNode.virtual));
		});

		// add query tree node's non-scalar children
		queryTreeNode.children.forEach(childQueryTreeNode => {
			if (childQueryTreeNode.many)
				noMoreChildExpansion |= this._assembleQueryBuilders(
					queryBuilders, noMoreChildExpansion, childQueryTreeNode,
					(forceOuter || childQueryTreeNode.virtual));
		});

		// tell the caller if adding the node made the query expanding
		return (queryTreeNode.many || noMoreChildExpansion);
	}

	/**
	 * Create new branch query builder.
	 *
	 * @private
	 * TODO: write jsdoc tags
	 */
	_createQueryBuilder(queryTreeNode) {

		// create query builder object
		const queryBuilder = {
			select: [],
			from: null,
			orderBy: [],
			groupBy: [],
			where: null,

			toSQL() {
				return 'SELECT ' +
					this.select.join(', ') +
					' FROM ' + this.from +
					(this.where ? ' WHERE ' + this.where : '') +
					(this.orderBy.length > 0 ? ' ORDER BY ' + this.orderBy.join(
						', ') : '') +
					(this.groupBy.length > 0 ? ' GROUP BY ' + this.groupBy.join(
						', ') : '') +
					' LIMIT 10'; // TODO: remove limit
			}
		};

		// add parent chain to the query
		const parentTreeNodeChain = new Array();
		for (let n = queryTreeNode.parent; n; n = n.parent)
			parentTreeNodeChain.push(n);
		let forceOuter = false;
		for (let i = parentTreeNodeChain.length - 1; i >= 0; i--) {
			const n = parentTreeNodeChain[i];
			this._addQueryTreeNodeToQueryBuilder(
				queryBuilder, n, forceOuter, true);
			forceOuter |= n.virtual;
		}

		// return the new query builder
		return queryBuilder;
	}

	/**
	 * Add query tree node's information to the query (without descending to the
	 * node children).
	 *
	 * @private
	 * TODO: write jsdoc tags
	 */
	_addQueryTreeNodeToQueryBuilder(
		queryBuilder, queryTreeNode, forceOuter, keysOnly) {

		// add node properties to the SELECT list
		queryBuilder.select.push.apply(
			queryBuilder.select,
			(keysOnly ? queryTreeNode.keys : queryTreeNode.props).map(
				p => p.expr + ' AS ' + this._dbDriver.safeLabel(p.markup))
		);

		// add node table to the FROM clause
		if (queryTreeNode.parent) {
			queryBuilder.from += (
				(forceOuter || queryTreeNode.virtual) ?
					' LEFT OUTER' : ' INNER') + ' JOIN ' +
				queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias +
				' ON ' + queryTreeNode.tableAlias + '.' +
				queryTreeNode.joinByColumn + ' = ' +
				queryTreeNode.parent.tableAlias + '.' +
				queryTreeNode.joinToColumn;
		} else {
			queryBuilder.from =
				queryTreeNode.table + ' AS ' + queryTreeNode.tableAlias;
		}

		// add parent key ORDER BY if collection
		if (queryTreeNode.many && queryTreeNode.parent)
			queryBuilder.orderBy.push(
				queryTreeNode.parent.tableAlias + '.' +
					queryTreeNode.parent.keyColumn);

		// TODO: add node's order, group and where
		//...
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
						this._querySpec.recordTypes,
						this._querySpec.topRecordTypeName
					);
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
			return this._dbDriver.stringLiteral(val);
		default:
			throw new common.X2UsageError(
				'Query parameter ' + paramName + ' has unsupported value type ' +
					(typeof val) + '.');
		}
	}
}

module.exports = Query;
