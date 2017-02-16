'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const placeholders = require('./placeholders.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * Node in the selected properties tree.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class PropertyTreeNode {

	/**
	 * Create new node.
	 *
	 * @param {?module:x2node-queries~PropertyTreeNode} parentNode Parent tree
	 * node, or <code>null</code> for the top node.
	 * @param {string} propPath Path of the property represented by the node as
	 * reported by the node's <code>path</code> property.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Descriptor of
	 * the property represented by the node.
	 * @param {module:x2node-records~PropertiesContainer[]} [containerChain]
	 * Container chain that can be used to create value expression context scoped
	 * to the property represented by the node. The last element in the chain is
	 * the container of the node property's children. Not used if the node's
	 * property is not a container (not an object nor a reference).
	 */
	constructor(parentNode, propPath, propDesc, containerChain) {

		this[PARENT_NODE] = parentNode;

		this._path = propPath;
		this._desc = propDesc;
		this._expanding = !propDesc.isScalar();
		if (containerChain) {
			this._containerChain = containerChain;
			this._childrenContainer = containerChain[containerChain.length - 1];
		}

		this._usedIn = new Set();
		this._includedPropPaths = new Set();
	}

	/**
	 * Property path associated with the node. The path is relative to the top
	 * query record.
	 *
	 * @type {string}
	 * @readonly
	 */
	get path() { return this._path; }

	/**
	 * Descriptor of the property represented by the node.
	 *
	 * @type {module:x2node-records~PropertyDescriptor}
	 * @readonly
	 */
	get desc() { return this._desc; }

	/**
	 * If the property is an object or a reference, this is container for child
	 * properties.
	 *
	 * @type {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get childrenContainer() { return this._childrenContainer; }

	/**
	 * Create context for value expressions relative to this node's property.
	 * Used for scoped filter and order specifications.
	 *
	 * @returns {module:x2node-queries~ValueExpressionContext} The new context.
	 */
	createValueExpressionContext() {

		return new ValueExpressionContext(this._path, this._containerChain);
	}

	/**
	 * Mark the node as used in the specified clause.
	 *
	 * @param {string} clause The clause ("select", "where", "orderBy" or
	 * "value").
	 */
	addClause(clause) {

		this._usedIn.add(clause);
	}

	/**
	 * Tell if the node is used in the specified clause.
	 *
	 * @param {string} clause The clause.
	 * @returns {boolean} <code>true</code> if used in the clause.
	 */
	isUsedIn(clause) { return this._usedIn.has(clause); }

	/**
	 * Tell if the node is used in the select clause (shortcut for
	 * <code>isUsedIn('select')</code>).
	 *
	 * @returns {boolean} <code>true</code> if used in the select clause.
	 */
	isSelected() { return this.isUsedIn('select'); }

	addChild(childPropName, childNode) {

		if (!this._children)
			this._children = new Map();

		this._children.set(childPropName, childNode);

		for (let n = this; n !== null; n = n[PARENT_NODE]) {
			n._includedPropPaths.add(childNode.path);
			childNode._includedPropPaths.forEach(p => {
				n._includedPropPaths.add(p);
			});
		}
	}

	hasChildren() { return (this._children && (this._children.size > 0)); }

	get children() {

		return (this._children ? this._children.values() : undefined);
	}

	getChild(childPropName) {

		return (this._children ? this._children.get(childPropName) : undefined);
	}

	includesProp(propPath) { return this._includedPropPaths.has(propPath); }

	get valueExpr() { return this._valueExpr; }
	set valueExpr(valueExpr) { this._valueExpr = valueExpr; }

	get filter() { return this._filter; }
	set filter(filter) { this._filter = filter; }

	get order() { return this._order; }
	set order(order) { this._order = order; }

	get expandingChild() { return this._expandingChild; }

	isExpanding() { return this._expanding; }

	clone(parentNode) {

		return new PropertyTreeNode(
			parentNode, this._path, this._desc, this._containerChain);
	}

	cloneForBranch(parentNode) {

		const node = this.clone(parentNode);
		node._usedIn = new Set(this._usedIn);
		node._valueExpr = this._valueExpr;
		node._filter = this._filter;
		node._order = this._order;
	}

	combineAndDebranch(otherTreeNode) {

		return this._combine(otherTreeNode).deranch();
	}

	_combine(parentNode, otherTreeNode) {

		const combined = this.cloneForBranch(parentNode);

		otherNode._usedIn.forEach(clause => {
			combined.addClause(clause);
		});

		if (this._children)
			this._children.forEach((childNode, childPropName) => {
				combined.addChild(childPropName, childNode);
			});
		if (otherNode._children)
			otherNode._children.forEach((childNode, childPropName) => {
				const n = combined.getChild(childPropName);
				if (n) {
					combined.addChild(childPropName, n._combine(
						combined, childNode));
				} else {
					combined.addChild(childPropName, childNode);
				}
			});

		return combined;
	}

	debranch() {

		const branches = new Array();

		this._debranch(null, this, branches);

		return branches;
	}

	_debranch(parentNode, node, branches) {

		let branch = node.cloneForBranch(parentNode);
		branches.push(branch);

		if (node.hasChildren()) {
			const childBranches = new Array();
			node._children.forEach((childNode, childPropName) => {
				childBranches.length = 0;
				childNode._debranch(branch, childNode, childBranches);
				childBranches.forEach(childBranch => {
					if (!childBranch.isExpanding())
						branch.addChild(childPropName, childBranch);
				});
				childBranches.forEach(childBranch => {
					if (childBranch.isExpanding()) {
						if (branch._expandingChild) {
							branch = node.cloneForBranch(parentNode);
							branches.push(branch);
						}
						branch.addChild(childPropName, childBranch);
						branch._expandingChild = childBranch;
						branch._expanding = true;
					}
				});
			});
		}
	}
}

/**
 * Query range specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QueryRangeSpec {

	constructor(offset, limit) {

		this._offset = offset;
		this._limit = limit;
	}

	get offset() { return this._offset; }

	get limit() { return this._limit; }
}

/**
 * Query specification.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class QuerySpec {

	/**
	 * Create new query specification object.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor
	 * of the top property, values of which are being fetched by the query. When
	 * the query specification is built for a record type, this is the descriptor
	 * of the "records" property in the super-type.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Context
	 * for any value expressions that appear in the filter and order
	 * specifications.
	 * @param {string[]} [selectedPropsSpec] Selected property patterns. If not
	 * specified, <code>['*']</code> is assumed.
	 * @param {Array[]} [filterSpec] Optional specification of the filter to
	 * apply to the selected records.
	 * @param {string[]} [orderSpec] Optional order specification to apply to the
	 * selected records.
	 * @param {number[]} [rangeSpec] Optional range specification to apply to the
	 * selected records.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specifications are invalid.
	 */
	constructor(
		recordTypes, topPropDesc, valueExprCtx, selectedPropsSpec, filterSpec,
		orderSpec, rangeSpec) {

		this._recordTypes = recordTypes;

		// create complete branching tree of selected properties
		const branchingSelectedPropsTree = new PropertyTreeNode(
			null, valueExprCtx.basePath, topPropDesc,
			valueExprCtx.containerChain);
		const scopedPropTrees = new Map();
		this._parseSelectedProps(
			branchingSelectedPropsTree,
			valueExprCtx,
			(selectedPropsSpec ? selectedPropsSpec : [ '*' ]),
			scopedPropTrees
		);

		// de-branch the selected properties tree and add scoped property trees
		const scopedPropTreesArray = Array.from(scopedPropTrees.entries());
		this._selectedPropsTrees = branchingSelectedPropsTree.debranch().map(
			selectedPropsTree => scopedPropTreesArray.reduce((res, pair) => {
				const scopePropPath = pair[0];
				const scopedPropTree = pair[1];
				if (!res.includesProp(scopePropPath))
					return res;
				const combinedTrees = res.combineAndDebranch(scopedPropTree);
				if (combinedTrees.length > 1)
					throw new common.X2UsageError(
						'Invalid property "' + scopePropPath +
							'" definition: refers to other properties that do' +
							' not lay on the same collection axis.');
				return combinedTrees[0];
			}, selectedPropsTree)
		);

		// parse top filter specification
		if (filterSpec) {
			const usedPropPaths = new Set();
			this._filter = filterBuilder.parseFilterSpec(
				recordTypes, topPropDesc, valueExprCtx, [ ':and', filterSpec ],
				usedPropPaths);
			if (this._filter) {
				this._filterPropsTree = branchingSelectedPropsTree.clone(null);
				usedPropPaths.forEach(p => {
					this._addProperty(
						this._filterPropsTree, this._filterPropsTree.path, p,
						'where');
				});
			}
		}

		// parse top order specification
		if (orderSpec) {
			const usedPropPaths = new Set();
			this._order = orderBuilder.parseOrderSpec(
				valueExprCtx, orderSpec, usedPropPaths);
			if (this._order) {
				this._orderPropsTree = topBranchingNode.clone(null);
				usedPropPaths.forEach(p => {
					this._addProperty(
						this._orderPropsTree, this._orderPropsTree.path, p,
						'orderBy');
				});
			}
		}

		// parse range specification
		if (rangeSpec)
			this._range = this._parseRange(rangeSpec);
	}

	/**
	 * Parse selected property patterns and build out the tree.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top selected
	 * properties tree node.
	 * @param {module:x2node-queries~ValueExpressionContext} valueExprCtx Value
	 * expression context to use.
	 * @param {string[]} propPatterns List of selected property patterns to
	 * parse.
	 * @param {external:Map.<string,module:x2node-queries~SingleAxisPropertyTreeNode>} scopedPropTrees
	 * Map, to which to add generated scoped property trees.
	 * @throws {module:x2node-common.X2UsageError} If the any of the patterns is
	 * invalid.
	 */
	_parseSelectedProps(topNode, valueExprCtx, propPatterns, scopedPropTrees) {

		// process direct patterns
		const excludedPaths = new Set();
		let wcPatterns = new Array();
		propPatterns.forEach(propPattern => {

			if (propPattern.startsWith('-')) { // collect exclusion pattern

				excludedPaths.add(propPattern.substring(1));

			} else { // process regular inclusion pattern

				this._addProperty(
					topNode, null, propPattern, 'select', scopedPropTrees,
					wcPatterns);
			}
		});

		// process wildcard patterns
		while (wcPatterns.length > 0) {
			const wcPatterns2 = new Array();
			wcPatterns.forEach(propPattern => {
				if (!excludedPaths.has(propPattern))
					this._addProperty(
						topNode, null, propPattern, 'select', scopedPropTrees,
						wcPatterns2);
			});
			wcPatterns = wcPatterns2;
		}
	}

	/**
	 * Add property to the selected properties tree.
	 *
	 * @private
	 * @param {module:x2node-queries~PropertyTreeNode} topNode Top selected
	 * properties tree node.
	 * @param {?string} scopeColPath Path of the scope collection property. If
	 * provided, the pattern may only belong to the scope collection property's
	 * axis.
	 * @param {string} propPattern Property pattern. May be a wildcard pattern if
	 * the <code>clause</code> argument is "select".
	 * @param {string} clause The query clause where the property is used.
	 * @param {string[]} wcPatterns Array, to which to add extra patterns
	 * resulting in the wildcard pattern expansion. Not used unless the
	 * <code>clause</code> argument is "select".
	 * @returns {module:x2node-queries~PropertyTreeNode} The leaf node
	 * representing the property.
	 */
	_addProperty(
		topNode, scopeColPath, propPattern, clause, scopedPropTrees,
		wcPatterns) {

		// check if adding to the SELECT clause
		const selected = (clause === 'select');

		// process the pattern parts
		let expandChildren = false;
		const propPatternParts = propPattern.split('.');
		const numParts = propPatternParts.length;
		let parentNode = topNode;
		let patternPrefix = topNode.path, patternSuffix = '';
		const containerChain = new Array();
		for (let i = 0; i < numParts; i++) {
			const propName = propPatternParts[i];
			const parentPropDesc = parentNode.desc;

			// get container from the parent node
			const container = parentNode.childrenContainer;
			if (!container)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern
						+ '": property ' + parentPropDesc.container.nestedPath +
						parentPropDesc.name + ' of ' +
						String(parentPropDesc.container.recordTypeName) +
						' is not an object nor reference and cannot be used as' +
						' an intermediate property in a path.');
			containerChain.push(container);

			// check if wildcard
			if (propName === '*') {

				// wildcards are only allowed for selected property patterns
				if (!selected)
					throw new common.X2UsageError(
						'Invalid property path "' + propPattern +
							'": wild cards are only allowed in selected' +
							' property patterns.');

				// set up the expansion
				expandChildren = true;
				if (i < numParts - 1) {
					patternSuffix =
						'.' + propPatternParts.slice(i + 1).join('.');
				}

				// done looping through pattern parts
				break;
			}

			// try to get existing node
			let node = parentNode.getChild(propName);

			// create new node is does not yet exist
			if (!node) {

				// get property descriptor
				if (!container.hasProperty(propName))
					throw new common.X2UsageError(
						'Invalid property path "' + propPattern +
							'": record type ' + container.recordTypeName +
							' does not have property ' + container.nestedPath +
							propName + '.');
				const propDesc = container.getPropertyDesc(propName);

				// determine property children container, if applicable
				let childrenContainer;
				switch (propDesc.scalarValueType) {
				case 'object':
					childrenContainer = propDesc.nestedProperties;
					break;
				case 'ref':
					childrenContainer =
						this._recordTypes.getRecordTypeDesc(propDesc.refTarget);
				}

				// create node for the property
				node = new PropertyTreeNode(
					parentNode,
					patternPrefix + propName,
					propDesc,
					(
						childrenContainer ?
							containerChain.concat(childrenContainer) :
							undefined
					)
				);

				// add node to the tree
				parentNode.addChild(propName, node);

				// TODO: aggregate may be a map

				// validate calculated or aggreate property usage
				if (propDef.valueExpr || propDef.aggregate) {

					// may only be selected
					if (!selected)
						throw new common.X2UsageError(
							'Invalid property path "' + propPattern +
								'": calculated value or aggregate property may' +
								' only be used in selected propery patterns.');

					// must be scalar, not an object and either expr or aggregate
					if (!propDesc.isScalar() ||
						(propDesc.scalarValueType === 'object') ||
						(propDef.valueExpr && propDef.aggregate))
						throw new common.X2UsageError(
							'Invalid definition of property ' +
								container.nestedPath + propName +
								' of record type ' +
								String(container.recordTypeName) +
								': calculated value and aggregate properties' +
								' may only be scalar, may not be objects and' +
								' may not be both calculated value and' +
								' aggregate at the same time.');

					// may only be a leaf
					if (i < numParts - 1)
						throw new common.X2UsageError(
							'Invalid selected property pattern "' + propPattern +
								'": calculated value or aggregate property may' +
								' not be used as in the middle of a pattern' +
								' but only at end of it.');
				}

				// logic for calculated properties
				const propDef = propDesc.definition;
				if (propDef.valueExpr) {

					// compile the property value expression
					const valueExprCtx = new ValueExpressionContext(
						parentNode.path, containerChain);
					node.valueExpr = new ValueExpression(
						valueExprCtx, propDef.valueExpr);

					// create scoped property tree
					const scopedPropTree = topNode.clone(null);
					node.valueExpr.usedPropertyPaths.forEach(p => {
						this._addProperty(
							scopedPropTree, node.propPath, p, 'value');
					});
					scopedPropTrees.set(node.propPath, scopedPropTree);

				} else if (propDef.aggregate) {

					// get aggregated collection path
					const aggColPath = propDef.aggregate.collection;
					if (!aggColPath)
						throw new common.X2UsageError(
							'Invalid definition of property ' +
								container.nestedPath + propName +
								' of record type ' +
								String(container.recordTypeName) +
								': aggregate must specify aggregated' +
								' collection.');

					// get aggregation expression
					// TODO: implement...

					throw new Error('Aggregates not implemented yet.');
				}

				// logic for scoped filtering and ordering
				if (selected && (propDef.filter || propDef.order)) {

					// must be a collection
					if (propDesc.isScalar())
						throw new common.X2UsageError(
							'Invalid definition of property ' +
								container.nestedPath + propName +
								' of record type ' +
								container.recordTypeName +
								': scoped filters and orders are only allowed' +
								' on non-scalar properties.');

					// create value expression context
					const valueExprCtx = node.createValueExpressionContext();

					// collect used properties
					const usedPropPaths = new Set();

					// process filter
					if (propDef.filter) {

						// must be a iview
						if (!propDesc.isView())
							throw new common.X2UsageError(
								'Invalid definition of property ' +
									container.nestedPath + propName +
									' of record type ' +
									container.recordTypeName +
									': scoped filters are only allowed on' +
									' views.');

						// parse the filter
						node.filter = filterBuilder.parseFilterSpec(
							this._recordTypes, topNode.desc, valueExprCtx,
							[ ':and', propDef.filter ], usedPropPaths);
					}

					// process order
					if (propDef.order)
						node.order = orderBuilder.parseOrderSpec(
							valueExprCtx, propDef.order, usedPropPaths);

					// create scoped property tree
					const scopedPropTree = topNode.clone(null);
					usedPropPaths.forEach(p => {
						this._addProperty(
							scopedPropTree, node.propPath, p, 'value');
					});
					scopedPropTrees.set(node.propPath, scopedPropTree);
				}
			}

			// make sure the node is on the scope collection axis
			if (scopeColPath && !node.desc.isScalar() &&
				!((scopeColPath === node.path) || scopeColPath.startsWith(
					node.path + '.')))
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": the property must be on the same collection axis' +
						' as ' + scopeColPath + '.');

			// set node usage flag
			node.addClause(clause);

			// add part to the reconstructed pattern prefix
			patternPrefix += propName + '.';

			// advance down the tree
			parentNode = node;
		}

		// expand selected object
		if (!expandChildren && (parentNode.desc.scalarValueType === 'object')) {

			// may not expand unless selected
			if (!selected)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": object properties are only allowed in selected' +
						' property patterns.');

			// set up the expansion
			expandChildren = true;
		}

		// generate expanded patterns
		if (expandChildren) {
			parentNode.childrenContainer.allPropertyNames.forEach(propName => {
				const propDesc = parentNode.childrenContainer.getPropertyDesc(
					propName);
				const propDef = propDesc.definition;
				if (((propDef.fetchByDefault === undefined) &&
						!propDesc.isView() && !propDef.valueExpr &&
							!propDef.aggregate) ||
					propDef.fetchByDefault)
					wcPatterns.push(patternPrefix + propName + patternSuffix);
			});
		}

		// return the leaf node
		return parentNode;
	}

	/**
	 * Parse raw query range specification.
	 *
	 * @private
	 * @param {number[]} rangeSpec Two-element number array of the raw range
	 * specification.
	 * @returns {module:x2node-queries~QueryRangeSpec} Range specification
	 * object.
	 * @throws {module:x2node-common.X2UsageError} If the specification is
	 * invalid.
	 */
	_parseRange(rangeSpec) {

		// validate range specification array
		if (!Array.isArray(rangeSpec) || (rangeSpec.length !== 2))
			throw new common.X2UsageError(
				'Query range specification is not a two-element array.');
		const offset = rangeSpec[0];
		const limit = rangeSpec[1];

		// validate range values
		if (((typeof offset) !== 'number') ||
			Number.isNaN(offset) || (offset < 0) ||
			((typeof limit) !== 'number') ||
			Number.isNaN(limit) || (limit < 0))
			throw new common.X2UsageError(
				'Invalid query range specification: the offset or the limit' +
					' value is not a number or is negative.');

		// create and return the range specification object
		return new QueryRangeSpec(offset, limit);
	}

	/**
	 * Single axis trees for all selected properties.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode[]}
	 * @readonly
	 */
	get selectedPropsTrees() { return this._selectedPropsTrees; }

	/**
	 * Top filter specification, or <code>undefined</code> if none.
	 *
	 * @type {module:x2node-queries~QueryFilterSpec}
	 * @readonly
	 */
	get filter() { return this._filter; }

	/**
	 * Tree of properties used in the top filter.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode}
	 * @readonly
	 */
	get filterPropsTree() { return this._filterPropsTree; }

	/**
	 * Top order specification, or <code>undefined</code> if none.
	 *
	 * @type {module:x2node-queries~QueryOrderSpec[]}
	 * @readonly
	 */
	get order() { return this._order; }

	/**
	 * Tree of properties used in the top order.
	 *
	 * @type {module:x2node-queries~PropertyTreeNode}
	 * @readonly
	 */
	get orderPropsTree() { return this._orderPropsTree; }

	/**
	 * Range spefification, or <code>undefined</code> if none. Range
	 * specification has two number properties: zero-based <code>offset</code>
	 * and <code>limit</code>.
	 *
	 * @type {Object.<string,number>}
	 * @readonly
	 */
	get range() { return this._range; }
}

module.exports = QuerySpec;
