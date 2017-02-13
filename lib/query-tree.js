'use strict';

const common = require('x2node-common');


// validation and other query tree building functions:

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
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' does not have a database table column associated with it.');
}

function mustHaveKeyColumn(propDesc) {
	if (!propDesc.definition.keyColumn)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
				' does not have a map key database table column' +
				' associated with it.');
}

function getIdColumn(container) {
	const idColumn = container.getPropertyDesc(
		container.idPropertyName).definition.column;
	if (!idColumn)
		throw new common.X2UsageError(
			'Property ' + container.nestedPath + container.idPropertyName +
				' of record type ' + container.recordTypeName +
				' does not have a database table column associated with it.');
	return idColumn;
}

function getKeyColumn(propDesc, keyPropContainer) {
	const propDef = propDesc.definition;
	if (propDef.keyColumn)
		return propDef.keyColumn;
	if (!propDesc.keyPropertyName)
		throw new common.X2UsageError(
			'Property ' + propDesc.container.nestedPath + propDesc.name +
				' of record type ' + propDesc.container.recordTypeName +
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
				' it and must be stored in the same table as the record.');
	return keyPropDesc.definition.column;
}

function getRefTargetDesc(recordTypes, propDesc) {
	const refTargetDesc = recordTypes.getRecordTypeDesc(propDesc.refTarget);
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


/**
 * The query tree node.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryTreeNode {

	/**
	 * Create new node. Used by the <code>createChildNode</code> method as well
	 * as once directly in the special case to create the super node (the parent
	 * of the top node).
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-queries.DBDriver} dbDriver Database driver.
	 * @param {?module:x2node-queries~QueryTreeNode} parentNode Parent node, or
	 * <code>null</code> to create the super node.
	 */
	constructor(recordTypes, dbDriver, parentNode) {

		this._recordTypes = recordTypes;
		this._dbDriver = dbDriver;

		this._superNode = !parentNode;

		if (!this._superNode) {

			this.parent = (parentNode._superNode ? null : parentNode);

			this._nextTableAliasDisc = 'a'.charCodeAt(0);

			this.keys = new Array();
			this.props = new Array();

			this.children = new Array();
		}
	}

	/**
	 * Create child node.
	 *
	 * @param {string} table The table, for which the node is being created.
	 * @param {boolean} many <code>true</code> if may result in multiple rows.
	 * @param {boolean} virtual <code>true</code> if may select no rows.
	 * @param {string} joinByColumn Name of the column in the node's table used
	 * to join to the parent table.
	 * @param {string} joinToColumn Name of the column in the parent node's table
	 * used for the join.
	 * @param {string} keyColumn Name of the column in the node's table that can
	 * be used to join children to.
	 * @returns {module:x2node-queries~QueryTreeNode} The new child node.
	 */
	createChildNode(
		table, many, virtual, joinByColumn, joinToColumn, keyColumn) {

		const childNode = new QueryTreeNode(
			this._recordTypes, this._dbDriver, this);
		childNode.table = table;
		childNode.tableAlias = (
			this._superNode ? 'z' :
				this.tableAlias + String.fromCharCode(
					this._nextTableAliasDisc++)
		);
		childNode.many = many;
		childNode.virtual = virtual;
		childNode.joinByColumn = joinByColumn;
		childNode.joinToColumn = joinToColumn;
		childNode.keyColumn = keyColumn;

		if (!this._superNode)
			this.children.push(childNode);

		return childNode;
	}

	/**
	 * Add selected property to the node.
	 *
	 * @param {Object} selectedProp Selected property node from the query
	 * specification tree.
	 * @param {Object} markupCtx Markup context for the property.
	 * @param {external:Map.<string,string>} propsToCols Map of property paths to
	 * corresponding SQL column expressions that is populated by this method.
	 */
	addSelectedProp(selectedProp, markupCtx, propsToCols) {

		// create markup prefix context for possible children
		const markupPrefix = markupCtx.prefix;
		const childrenMarkupCtx = (
			selectedProp.hasChildren() ? {
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
		let fetch, keyColumn, anchor, valueExpr;
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

				// create child node for the value table
				queryTreeNode = this.createChildNode(
					propDef.table, false, propDesc.optional,
					propDef.parentIdColumn, this.keyColumn, null);

			} else { // stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// add value to the same query tree node
				queryTreeNode = this;
			}

			// save value mapping
			valueExpr = queryTreeNode.tableAlias + '.' + propDef.column;
			propsToCols.set(selectedProp.path, valueExpr);

			// add value selector
			if (selectedProp.isUsedIn('select'))
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix + propDesc.name
				});

			break;

		case 'scalar:object:mono':

			// check if stored in a separate table
			if (propDef.table) {

				// validate definition
				mustHaveParentIdColumn(propDesc);

				// create child node for the object table
				queryTreeNode = this.createChildNode(
					propDef.table, false, propDesc.optional,
					propDef.parentIdColumn, this.keyColumn,
					propDef.parentIdColumn);

				// create anchor selector
				anchor = {
					expr: queryTreeNode.tableAlias + '.' +
						queryTreeNode.keyColumn,
					markup: markupPrefix + propDesc.name
				};

			} else { // stored in the parent table

				// TODO: support presence expression
				if (propDesc.optional)
					throw new common.X2UsageError(
						'Property ' + propDesc.container.nestedPath +
							propDesc.name + ' of record type ' +
							propDesc.container.recordTypeName +
							' may not be optional as it is stored in the' +
							' parent record table.');

				// create anchor selector
				anchor = {
					expr: this._dbDriver.booleanLiteral(true),
					markup: markupPrefix + propDesc.name
				};

				// add child properties to the same node
				queryTreeNode = this;
			}

			// add anchor selector
			if (selectedProp.isUsedIn('select')) {
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

			// add selected child properties
			for (let p of selectedProp.children)
				queryTreeNode.addSelectedProp(p, childrenMarkupCtx, propsToCols);

			break;

		case 'scalar:object:poly':

			// TODO:...

			break;

		case 'scalar:ref:mono':

			// get target record info
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetIdColumn = getIdColumn(refTargetDesc);

			// check if fetched
			fetch = selectedProp.hasChildren();

			// check if dependent reference
			if (propDef.reverseRefProperty) {

				// get reverse reference property descriptor
				const reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				const reverseRefPropDef = reverseRefPropDesc.definition;

				// check how the reverse reference property is stored
				if (reverseRefPropDef.table) {

					this._addLinkTableScalarRefProp(
						selectedProp, propsToCols, reverseRefPropDesc,
						reverseRefPropDef.column,
						reverseRefPropDef.parentIdColumn, fetch, markupPrefix,
						refTargetDesc, refTargetIdColumn);

				} else { // stored in the target record table

					// validate definition
					mustHaveColumn(reverseRefPropDesc);

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, false,
						propDesc.optional,
						reverseRefPropDef.column, this.keyColumn,
						refTargetIdColumn);

					// save value mapping
					valueExpr =
						queryTreeNode.tableAlias + '.' + refTargetIdColumn;
					propsToCols.set(selectedProp.path, valueExpr);

					// add selected values
					if (selectedProp.isUsedIn('select')) {

						// add anchor/value selector
						anchor = {
							expr: valueExpr,
							markup: markupPrefix + propDesc.name +
								(fetch ? ':' : '')
						};
						queryTreeNode.props.push(anchor);

						// add anchor to the keys if fetched
						if (fetch)
							queryTreeNode.keys.push(anchor);
					}
				}

			} else if (propDef.table) { // direct reference in a link table

				this._addLinkTableScalarRefProp(
					selectedProp, propsToCols, propDesc, propDef.parentIdColumn,
					propDef.column, fetch, markupPrefix, refTargetDesc,
					refTargetIdColumn);

			} else { // direct reference stored in the parent table

				// validate definition
				mustHaveColumn(propDesc);

				// save value mapping
				valueExpr = this.tableAlias + '.' + propDef.column;
				propsToCols.set(selectedProp.path, valueExpr);

				// create anchor/value selector
				anchor = {
					expr: valueExpr,
					markup: markupPrefix + propDesc.name +
						(fetch ? ':' : '')
				};

				// add child node if fetched
				if (fetch) {

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, false,
						propDesc.optional,
						refTargetIdColumn, propDef.column,
						refTargetIdColumn);

					// add anchor and value
					if (selectedProp.isUsedIn('select')) {
						queryTreeNode.keys.push(anchor);
						queryTreeNode.props.push(anchor);
					}

				} else { // no fetch

					// add value
					if (selectedProp.isUsedIn('select'))
						this.props.push(anchor);
				}
			}

			// add fetched target properties
			if (fetch)
				for (let p of selectedProp.children)
					queryTreeNode.addSelectedProp(
						p, childrenMarkupCtx, propsToCols);

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

			// create child node for the values table
			queryTreeNode = this.createChildNode(
				propDef.table, true, propDesc.optional,
				propDef.parentIdColumn, this.keyColumn, null);

			// save value mapping
			valueExpr = queryTreeNode.tableAlias + '.' + propDef.column;
			propsToCols.set(selectedProp.path, valueExpr);

			// add anchor and value selectors
			if (selectedProp.isUsedIn('select')) {
				queryTreeNode.props.push({
					expr: queryTreeNode.tableAlias + '.' + (
						propDesc.isMap() ? propDef.keyColumn :
							propDef.parentIdColumn),
					markup: markupPrefix + propDesc.name
				});
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix.substring(0, markupPrefix.length - 1) +
						String.fromCharCode(markupCtx.nextChildMarkupDisc++) +
						'$'
				});
			}

			break;

		case 'array:object:mono':
		case 'map:object:mono':

			// validate definition
			mustHaveTable(propDesc);

			// create child node for the objects table
			keyColumn = (
				propDesc.isMap() ?
					getKeyColumn(propDesc, propDesc.nestedProperties) :
					getIdColumn(propDesc.nestedProperties)
			);
			queryTreeNode = this.createChildNode(
				propDef.table, true, propDesc.optional,
				propDef.parentIdColumn, this.keyColumn,
				keyColumn);

			// add anchor selector
			if (selectedProp.isUsedIn('select')) {
				anchor = {
					expr: queryTreeNode.tableAlias + '.' + keyColumn,
					markup: markupPrefix + propDesc.name
				};
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

			// add selected child properties
			for (let p of selectedProp.children)
				queryTreeNode.addSelectedProp(p, childrenMarkupCtx, propsToCols);

			break;

		case 'array:object:poly':
		case 'map:object:poly':

			// TODO:...

			break;

		case 'array:ref:mono':
		case 'map:ref:mono':

			// get target record info
			refTargetDesc = getRefTargetDesc(this._recordTypes, propDesc);
			refTargetIdColumn = getIdColumn(refTargetDesc);

			// check if fetched
			fetch = selectedProp.hasChildren();

			// check if dependent reference
			if (propDef.reverseRefProperty) {

				// get reverse reference property descriptor
				const reverseRefPropDesc = getReverseRefPropDesc(
					propDesc, refTargetDesc);
				const reverseRefPropDef = reverseRefPropDesc.definition;

				// check how the reverse reference property is stored
				if (reverseRefPropDef.table) {

					this._addLinkTableCollectionRefProp(
						selectedProp, propsToCols, reverseRefPropDesc,
						reverseRefPropDef.column,
						reverseRefPropDef.parentIdColumn, fetch, markupCtx,
						refTargetDesc, refTargetIdColumn, childrenMarkupCtx);

				} else { // stored in the target record table

					// validate definition
					mustHaveColumn(reverseRefPropDesc);

					// create child node for the referred records table
					queryTreeNode = this.createChildNode(
						refTargetDesc.definition.table, true,
						propDesc.optional,
						reverseRefPropDef.column, this.keyColumn,
						refTargetIdColumn);

					// save value mapping
					valueExpr =
						queryTreeNode.tableAlias + '.' + refTargetIdColumn;
					propsToCols.set(selectedProp.path, valueExpr);

					// add selected values
					if (selectedProp.isUsedIn('select')) {

						// add anchor selector
						anchor = {
							expr: queryTreeNode.tableAlias + '.' +  (
								propDesc.isMap() ?
									getKeyColumn(propDesc, refTargetDesc) :
									refTargetIdColumn
							),
							markup: markupPrefix + propDesc.name +
								(fetch ? ':' : '')
						};
						queryTreeNode.props.push(anchor);

						// add value selector(s)
						if (fetch) {

							// add anchor to the keys
							queryTreeNode.keys.push(anchor);

							// add id property selector
							const idSelector = {
								expr: valueExpr,
								markup: childrenMarkupCtx.prefix +
									refTargetDesc.idPropertyName
							};
							queryTreeNode.keys.push(idSelector);
							queryTreeNode.props.push(idSelector);

						} else { // no fetch

							// add value selector
							queryTreeNode.props.push({
								expr: valueExpr,
								markup: markupPrefix.substring(
									0, markupPrefix.length - 1) +
									String.fromCharCode(
										markupCtx.nextChildMarkupDisc++) + '$'
							});
						}
					}

					// add the rest of the fetched properties
					if (fetch)
						for (let p of selectedProp.children)
							if (!p.desc.isId())
								queryTreeNode.addSelectedProp(
									p, childrenMarkupCtx, propsToCols);
				}

			} else { // direct reference (must have link table)

				this._addLinkTableCollectionRefProp(
					selectedProp, propsToCols, propDesc, propDef.parentIdColumn,
					propDef.column, fetch, markupCtx, refTargetDesc,
					refTargetIdColumn, childrenMarkupCtx);
			}

			break;

		case 'array:ref:poly':
		case 'map:ref:poly':

			// TODO:...
		}
	}

	/**
	 * Add scalar reference property attached via a link table.
	 *
	 * @private
	 * @todo describe params
	 */
	_addLinkTableScalarRefProp(
		selectedProp, propsToCols, linkTablePropDesc, linkTableJoinByColumn,
		linkTableKeyColumn, fetch, markupPrefix, refTargetDesc,
		refTargetIdColumn) {

		const propDesc = selectedProp.desc;

		// validate definition
		mustHaveColumn(linkTablePropDesc);
		mustHaveParentIdColumn(linkTablePropDesc);

		// create child node for the link table
		const queryTreeNode = this.createChildNode(
			linkTablePropDesc.definition.table, false,
			propDesc.optional,
			linkTableJoinByColumn, this.keyColumn,
			linkTableKeyColumn);

		// save value mapping
		const valueExpr =
			queryTreeNode.tableAlias + '.' + queryTreeNode.keyColumn;
		propsToCols.set(selectedProp.path, valueExpr);

		// create anchor/value selector
		const anchor = {
			expr: valueExpr,
			markup: markupPrefix + propDesc.name + (fetch ? ':' : '')
		};

		// add child node if fetched
		if (fetch) {

			// create child node for the referred records table
			queryTreeNode = queryTreeNode.createChildNode(
				refTargetDesc.definition.table, false, queryTreeNode.virtual,
				refTargetIdColumn, queryTreeNode.keyColumn,
				refTargetIdColumn);

			// add anchor and value
			if (selectedProp.isUsedIn('select')) {
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);
			}

		} else { // no fetch

			// add value
			if (selectedProp.isUsedIn('select'))
				queryTreeNode.props.push(anchor);
		}
	}

	/**
	 * Add collection reference property attached via a link table.
	 *
	 * @private
	 * @todo describe params
	 */
	_addLinkTableCollectionRefProp(
		selectedProp, propsToCols, linkTablePropDesc, linkTableJoinByColumn,
		linkTableKeyColumn, fetch, markupCtx, refTargetDesc, refTargetIdColumn,
		childrenMarkupCtx) {

		const propDesc = selectedProp.desc;
		const propDef = propDesc.definition;
		const markupPrefix = markupCtx.prefix;

		// validate definition
		mustHaveTable(linkTablePropDesc);
		mustHaveColumn(linkTablePropDesc);

		// create child node for the link table
		let queryTreeNode = this.createChildNode(
			linkTablePropDesc.definition.table, true,
			propDesc.optional,
			linkTableJoinByColumn, this.keyColumn,
			linkTableKeyColumn);

		// create anchor selector
		const anchor = {
			markup: markupPrefix + propDesc.name + (fetch ? ':' : '')
		};
		let targetTableAdded = false, keyColumn;
		if (propDesc.isMap()) {
			keyColumn = getKeyColumn(propDesc, refTargetDesc);
			if (propDesc.keyPropertyName) {
				queryTreeNode = queryTreeNode.createChildNode(
					refTargetDesc.definition.table, false, false,
					refTargetIdColumn, queryTreeNode.keyColumn,
					refTargetIdColumn);
				targetTableAdded = true;
			}
		} else { // array
			keyColumn = (
				propDef.keyColumn ? propDef.keyColumn : linkTableKeyColumn);
		}
		anchor.expr = queryTreeNode.tableAlias + '.' + keyColumn;

		// add value column(s)
		let valueExpr;
		if (fetch) {

			// create node for the referred records table if necessary
			if (!targetTableAdded)
				queryTreeNode = queryTreeNode.createChildNode(
					refTargetDesc.definition.table, false, false,
					refTargetIdColumn, queryTreeNode.keyColumn,
					refTargetIdColumn);

			// create value expression
			valueExpr = queryTreeNode.tableAlias + '.' + refTargetIdColumn;

			// add selected values
			if (selectedProp.isUsedIn('select')) {

				// add anchor and value selectors
				queryTreeNode.keys.push(anchor);
				queryTreeNode.props.push(anchor);

				// add id property selector
				const idSelector = {
					expr: valueExpr,
					markup: childrenMarkupCtx.prefix +
						refTargetDesc.idPropertyName
				};
				queryTreeNode.keys.push(idSelector);
				queryTreeNode.props.push(idSelector);
			}

			// add the rest of the fetched properties
			for (let p of selectedProp.children)
				if (!p.desc.isId())
					queryTreeNode.addSelectedProp(
						p, childrenMarkupCtx, propsToCols);

		} else { // no fetch

			// create value expression
			valueExpr = queryTreeNode.tableAlias + '.' + (
				targetTableAdded ? refTargetIdColumn : linkTableKeyColumn);

			// add anchor and value selectors
			if (selectedProp.isUsedIn('select')) {
				queryTreeNode.props.push(anchor);
				queryTreeNode.props.push({
					expr: valueExpr,
					markup: markupPrefix.substring(0, markupPrefix.length - 1) +
						String.fromCharCode(markupCtx.nextChildMarkupDisc++) +
						'$'
				});
			}
		}

		// save value mapping
		propsToCols.set(selectedProp.path, valueExpr);
	}
}

/**
 * Build query tree.
 *
 * @private
 * @param {module:x2node-queries.DBDriver} dbDriver The database driver.
 * @param {module:x2node-queries~QuerySpec} querySpec Query specification.
 * @returns {module:x2node-queries~QueryTreeNode} Top node of the built query
 * tree.
 * @throws {module:x2node-common.X2UsageError} If the provided query
 * specification is invalid or the underlying record types library is not
 * suitable for it.
 */
exports.build = function(dbDriver, querySpec) {

	// get and validate top records specification data
	const recordsProp = querySpec.selectedProps.get('records');
	const recordsPropDesc = recordsProp.desc;
	const recordsPropDef = recordsPropDesc.definition;
	if (!recordsPropDef.table)
		throw new common.X2UsageError(
			'Specified top record type ' + querySpec.topRecordTypeName +
				' does not have database table associated with it.');
	const topIdPropName = recordsPropDesc.nestedProperties.idPropertyName;
	const topIdColumn = recordsPropDesc.nestedProperties.getPropertyDesc(
		topIdPropName).definition.column;
	if (!topIdColumn)
		throw new common.X2UsageError(
			'Id property of the specified top record type ' +
				querySpec.topRecordTypeName +
				' does not have a database table column associated with it.');

	// property to column mappings
	const propsToCols = new Map();

	// create top tree node
	const superNode = new QueryTreeNode(querySpec.recordTypes, dbDriver, null);
	const queryTree = superNode.createChildNode(
		recordsPropDef.table, true, false, null, null, topIdColumn);

	// add top record id property to have it in front of the select list
	const topIdProp = {
		expr: queryTree.tableAlias + '.' + topIdColumn,
		markup: topIdPropName
	};
	queryTree.keys.push(topIdProp);
	queryTree.props.push(topIdProp);
	propsToCols.set(topIdPropName, topIdProp.expr);

	// add the rest of selected properties
	const topMarkupCtx = {
		prefix: '',
		nextChildMarkupDisc: 'a'.charCodeAt(0)
	};
	for (let p of recordsProp.children)
		if (!p.desc.isId()) // already included
			queryTree.addSelectedProp(p, topMarkupCtx, propsToCols);

	// add top order
	queryTree.order = recordsProp.order;

	// add the properties mapping
	queryTree.propsToCols = propsToCols;

	// return the query tree
	return queryTree;
};
