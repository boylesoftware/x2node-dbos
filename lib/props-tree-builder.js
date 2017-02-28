'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');
const ValueExpression = require('./value-expression.js');
const filterBuilder = require('./filter-builder.js');
const orderBuilder = require('./order-builder.js');


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * Properties tree node.
 *
 * @private
 * @memberof module:x2node-queries
 * @inner
 */
class PropertyTreeNode {

	/**
	 * Create new node. Not used from outside of the class.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} propPath Property path including the base value expression
	 * context's base path.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Descriptor of
	 * the property represented by the node.
	 * @param {module:x2node-records~PropertiesContainer[]} containerChain Chain
	 * of property containers leading to the property. The first element in the
	 * chain is the container corresponding to the property with empty path
	 * (always the same container as the first one in the base value expression
	 * context's container chain). The last element is the container of the node
	 * property's children, or <code>null</code> if the property cannot have
	 * children (not an object nor a reference).
	 * @param {external:Set.<string>} clauses The initial clauses set.
	 */
	constructor(recordTypes, propPath, propDesc, containerChain, clauses) {

		this._recordTypes = recordTypes;
		this._path = propPath;
		this._desc = propDesc;
		this._containerChain = containerChain;

		this._expanding = !propDesc.isScalar();

		this._clauses = clauses;

		this._includedProps = new Map();

		this._childrenContainer = containerChain[containerChain.length - 1];
		if (this._childrenContainer)
			this._children = new Map();
	}

	/**
	 * Create top node of a tree.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor
	 * of the top property.
	 * @param {module:x2node-queries~ValueExpressionContext} baseValueExprCtx
	 * Base value expressions context.
	 * @param {string} [clause] Optional clause to associate the node with.
	 * @returns {module:x2node-queries~PropertyTreeNode} The top tree node.
	 */
	static createTopNode(recordTypes, topPropDesc, baseValueExprCtx, clause) {

		const topNode = new PropertyTreeNode(
			recordTypes, baseValueExprCtx.basePath, topPropDesc,
			baseValueExprCtx.containerChain, new Set());

		if (clause)
			topNode.addClause(clause);

		return topNode;
	}

	/**
	 * Create and add a child property node to this node.
	 *
	 * @param {string} propName Property name. Must be present in the node's
	 * children container.
	 * @param {string} clause Clause to associate the new child node with.
	 * @param {string} [scopePropPath] Scope property path.
	 * @param {Object} [options] Tree options.
	 * @param {string} srcPropPattern Property pattern that caused adding the
	 * child node. Used only for error messages.
	 * @returns {module:x2node-queries~PropertyTreeNode} The new child node.
	 * @param {external:Map.<string,module:x2node-queries~PropertyTreeNode>} valuePropsTrees
	 * Map, to which to add generated value property trees.
	 */
	addChild(
		propName, clause, scopePropPath, options, srcPropPattern,
		valuePropsTrees) {

		// invalid pattern function
		const invalidPropPattern = msg => new common.X2UsageError(
			'Invalid property path or pattern "' + srcPropPattern + '": ' + msg);

		// make sure the property can have children
		if (!this._childrenContainer)
			throw invalidPropPattern(
				'property ' + this._desc.container.nestedPath +
					this._desc.name + ' of ' +
					String(this._desc.container.recordTypeName) +
					' is not an object nor reference and cannot be used as' +
					' an intermediate element in a path.');

		// may not use children of a calculated property
		if (this._valueExpr)
			throw invalidPropPattern(
				'calculated value or aggregate property cannot be used as' +
					' an intermediate element in a path.');

		// get property descriptor
		if (!this._childrenContainer.hasProperty(propName))
			throw invalidPropPattern(
				'record type ' + this._childrenContainer.recordTypeName +
					' does not have property ' +
					this._childrenContainer.nestedPath + propName + '.');
		const propDesc = this._childrenContainer.getPropertyDesc(propName);

		// build property path
		const propPath = (
			this._path.length > 0 ? this._path + '.' + propName : propName);

		// if collection, make sure it's in the scope
		if (scopePropPath && !propDesc.isScalar() && !(
			(scopePropPath === propPath) || scopePropPath.startsWith(
				propPath + '.')))
			throw invalidPropPattern(
				'must lie on the same collection axis with ' +
					scopePropPath + '.');

		// determine property children container, if applicable
		let childrenContainer = null;
		switch (propDesc.scalarValueType) {
		case 'object':
			childrenContainer = propDesc.nestedProperties;
			break;
		case 'ref':
			childrenContainer = this._recordTypes.getRecordTypeDesc(
				propDesc.refTarget);
		}

		// create the child node
		const childNode = new PropertyTreeNode(
			this._recordTypes,
			propPath,
			propDesc,
			this._containerChain.concat(childrenContainer),
			new Set()
		);
		childNode[PARENT_NODE] = this;

		// add the clause to the child node
		childNode.addClause(clause);

		// add the child node to the tree and find the top node
		let topNode;
		this._children.set(propName, childNode);
		for (let n = this; n; n = n[PARENT_NODE]) {
			topNode = n;
			n._includedProps.set(propPath, childNode);
		}

		// process property definition:

		// invalid property definition function
		const invalidPropDef = msg => new common.X2UsageError(
			'Property ' + this._childrenContainer.nestedPath + propName +
				' of record type ' + this._childrenContainer.recordTypeName +
				' has invalid definition: ' + msg);

		// get the definition
		const propDef = propDesc.definition;

		// check if calculated value property
		if (propDef.valueExpr) {

			// check if allowed in the options
			if (options && options.noCalculated)
				throw invalidPropPattern(
					'calculated value properties are not allowed here.');

			// validate property definition
			if (propDef.aggregate || propDef.table || propDef.column ||
				propDef.presenceTest || propDef.order || propDef.filter ||
				propDef.reverseRefProperty || !propDesc.isScalar() ||
				(propDesc.scalarValueType === 'object'))
				throw invalidPropDef(
					'conflicting calculated value property definition' +
						' attributes or invalid property value type.');

			// compile the property value expression
			childNode._valueExpr = new ValueExpression(
				new ValueExpressionContext(this._path, this._containerChain),
				propDef.valueExpr
			);

			// create the value tree
			valuePropsTrees.set(propPath, buildPropsTreeBranches(
				this._recordTypes, topNode.desc, 'value',
				new ValueExpressionContext(
					topNode._path, topNode._containerChain),
				this._path, childNode._valueExpr.usedPropertyPaths, {
					noWildcards: true,
					noCalculated: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);
		}

		// check if aggregate property
		if (propDef.aggregate) {

			// check if allowed in the options
			if (options && (options.noCalculated || options.noAggregates))
				throw invalidPropPattern(
					'aggregate properties are not allowed here.');

			// validate property definition
			if (propDef.table || propDef.column ||
				propDef.presenceTest || propDef.order || propDef.filter ||
				propDef.reverseRefProperty ||
				(!propDesc.isScalar() && !propDesc.isMap()) ||
				(propDesc.scalarValueType === 'object'))
				throw invalidPropDef(
					'conflicting aggregate property definition' +
						' attributes or invalid property value type.');

			// check if has needed attributes
			const aggColPath = propDef.aggregate.collection;
			const valueExprSpec = propDef.aggregate.valueExpr;
			if (!aggColPath || !valueExprSpec)
				throw invalidPropDef(
					'aggregate definition attribute must have collection and' +
						' valueExpr properties.');

			// parse value expression
			const valueExprSpecParts = valueExprSpec.match(
					/^\s*([^=\s].*?)\s*=>\s*(count|sum|min|max|avg)\s*$/i);
			if (valueExprSpecParts === null)
				throw invalidPropDef(
					'invalid aggregated value expression syntax.');
			childNode._aggregateFunc = valueExprSpecParts[2].toUpperCase();
			const fullAggColPath = (
				this._path.length > 0 ?
					this._path + '.' + aggColPath : aggColPath);
			childNode._valueExpr = new ValueExpression(
				new ValueExpressionContext(fullAggColPath, this._containerChain),
				valueExprSpecParts[1]
			);

			// create the value tree
			valuePropsTrees.set(propPath, buildPropsTreeBranches(
				this._recordTypes, topNode.desc, 'value',
				new ValueExpressionContext(
					topNode._path, topNode._containerChain),
				fullAggColPath, childNode._valueExpr.usedPropertyPaths, {
					noWildcards: true,
					noCalculated: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);
		}

		// check if has a presense test
		if (!options || !options.ignorePresenceTestsOn ||
			!options.ignorePresenceTestsOn.has(propPath)) {
			if (propDef.presenceTest) {

				// validate the definition
				if (!propDesc.isScalar() ||
					(propDesc.scalarValueType !== 'object') ||
					!propDesc.optional || propDef.table)
					throw invalidPropDef(
						'presence test may only be specified on an optional' +
							' scalar object property stored in the parent' +
							' record\'s table.');

				// parse the test
				const usedPropPaths = new Set();
				childNode._presenceTest = filterBuilder.buildFilter(
					this._recordTypes,
					(colPropPath, propPaths) => buildPropsTreeBranches(
						this._recordTypes, topNode.desc, 'where',
						new ValueExpressionContext(
							topNode._path, topNode._containerChain),
						colPropPath,
						Array.from(propPaths).map(p => colPropPath + '.' + p), {
							noWildcards: true,
							noAggregates: true,
							ignoreScopedOrders: true,
							noScopedFilters: true,
							includeScopeProp: true
						}
					)[0],
					topNode.desc,
					new ValueExpressionContext(
						propPath, childNode._containerChain),
					[ ':and', propDef.presenceTest ],
					usedPropPaths
				);

				// create the value tree
				valuePropsTrees.set(propPath, buildPropsTreeBranches(
					this._recordTypes, topNode.desc, 'value',
					new ValueExpressionContext(
						topNode._path, topNode._containerChain),
					propPath, usedPropPaths, {
						noWildcards: true,
						noCalculated: true,
						ignoreScopedOrders: true,
						noScopedFilters: true,
						ignorePresenceTestsOn: new Set(
							options && options.ignorePresenceTestsOn ?
								Array.from(options.ignorePresenceTestsOn).concat(
									propPath) :
								[ propPath ]
						)
					})[0]);

			} else if ( // check if the presence test is required
				propDesc.isScalar() &&
					(propDesc.scalarValueType === 'object') &&
					propDesc.optional && !propDef.table) {
				throw invalidPropDef(
					'optional scalar object property stored in the parent' +
						' record\'s table must have a presence test associated' +
						' with it.');
			}
		}

		// check if has scoped filter
		const orderAndFilterPropPaths = new Set();
		if (propDef.filter) {

			// check if allowed in the options
			if (options && options.noScopedFilters)
				throw invalidPropPattern(
					'properties with scoped filters are not allowed here.');

			// validate the definition
			if (propDesc.isScalar() || !propDesc.isView())
				throw invalidPropDef(
					'scoped filters are only allowed on non-scalar view' +
						' properties.');

			// parse the filter
			childNode._filter = filterBuilder.buildFilter(
				this._recordTypes,
				(colPropPath, propPaths) => buildPropsTreeBranches(
					this._recordTypes, topNode.desc, 'where',
					new ValueExpressionContext(
						topNode._path, topNode._containerChain),
					colPropPath,
					Array.from(propPaths).map(p => colPropPath + '.' + p), {
						noWildcards: true,
						noAggregates: true,
						ignoreScopedOrders: true,
						noScopedFilters: true,
						includeScopeProp: true
					}
				)[0],
				topNode.desc,
				new ValueExpressionContext(propPath, childNode._containerChain),
				[ ':and', propDef.filter ],
				orderAndFilterPropPaths);
		}

		// check if has scoped order
		if (propDef.order && !(options && options.ignoreScopedOrders)) {

			// must be a collection
			if (propDesc.isScalar())
				throw invalidPropDef(
					'scoped orders are only allowed on non-scalar properties.');

			// parse the order
			childNode._order = orderBuilder.buildOrder(
				new ValueExpressionContext(propPath, childNode._containerChain),
				propDef.order,
				orderAndFilterPropPaths);
		}

		// create the value tree for the scoped filter and order
		if (orderAndFilterPropPaths.size > 0)
			valuePropsTrees.set(propPath, buildPropsTreeBranches(
				this._recordTypes, topNode.desc, 'value',
				new ValueExpressionContext(
					topNode._path, topNode._containerChain),
				propPath, orderAndFilterPropPaths, {
					noWildcards: true,
					noCalculated: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);

		// update this node's branching flags
		if (childNode._expanding && !this._expandingChild) {
			this._expandingChild = childNode;
			for (let n = this; n; n = n[PARENT_NODE])
				n._expanding = true;
		}
		if (childNode._aggregateFunc && !this._hasAggregates)
			for (let n = this; n; n = n[PARENT_NODE])
				n._hasAggregates = true;

		// return the child node
		return childNode;
	}

	/**
	 * Mark the node as used in the specified clause.
	 *
	 * @param {string} clause The clause.
	 */
	addClause(clause) {

		this._clauses.add(clause);
	}

	/**
	 * Create new trees, called branches, by cloning this node, using it as
	 * the top node for each branch and recursively proceeding into its children.
	 * All properties included in a single branch are guaranteed to lay on a
	 * single collection axis. The method does not change this tree.
	 *
	 * @param {Array} [branches] Array, to which to add the generated branches.
	 * If not provided, new array is automatically created.
	 * @returns {module:x2node-queries~PropertyTreeNode[]} The array containing
	 * the generated branches.
	 */
	debranch(branches) {

		// create output branches array if was not provided with the call
		if (!branches)
			branches = new Array();

		// create initial branch from this node
		let branch = this._cloneWithoutChildren();
		branches.push(branch);

		// process node children
		if (this._children) {

			// find expanding branches, add non-expanding, cluster aggregates
			const expandingBranches = new Array();
			const aggregateClusters = new Array();
			const childBranches = new Array();
			this._children.forEach((childNode, childPropName) => {
				if (childNode._aggregateFunc || childNode._hasAggregates) {
					let aggregateCluster = aggregateClusters.find(
						c => c[0].isCompatibleAggregate(childNode));
					if (!aggregateCluster)
						aggregateClusters.push(aggregateCluster = new Array());
					aggregateCluster.push(childNode);
				} else {
					childNode
						.debranch((childBranches.length = 0, childBranches))
						.forEach(childBranch => {
							if (!childBranch._expanding) {
								branch.addChildNode(childBranch);
							} else {
								expandingBranches.push(childBranch);
							}
						});
				}
			});

			// add aggregate children
			aggregateClusters.forEach(aggregateCluster => {
				if (branch._hasAggregates) {
					branch = this._cloneWithoutChildren();
					branches.push(branch);
				}
				aggregateCluster.forEach(childNode => {
					branch.addChildNode(childNode);
				});
			});

			// add expanding children
			let curBranchInd = 0;
			branch = branches[curBranchInd];
			expandingBranches.forEach(childBranch => {
				while (branch._expandingChild || branch._hasAggregates) {
					if (++curBranchInd < branches.length) {
						branch = branches[curBranchInd];
					} else {
						branch = this._cloneWithoutChildren();
						branches.push(branch);
					}
				}
				branch.addChildNode(childBranch);
			});
		}

		// return the branches
		return branches;
	}

	/**
	 * Add child node to this node.
	 *
	 * @param {module:x2node-queries~PropertyTreeNode} childNode The child node.
	 */
	addChildNode(childNode) {

		childNode[PARENT_NODE] = this;

		if (childNode._includedProps)
			childNode._includedProps.forEach((n, p) => {
				this._includedProps.set(p, n);
			});
		this._includedProps.set(childNode._path, childNode);

		this._children.set(childNode._desc.name, childNode);

		if (childNode._expanding && !this._expandingChild) {
			this._expandingChild = childNode;
			for (let n = this; n; n = n[PARENT_NODE])
				n._expanding = true;
		}

		if (childNode._aggregateFunc || childNode._hasAggregates)
			for (let n = this; n; n = n[PARENT_NODE])
				n._hasAggregates = true;
	}

	/**
	 * Tell if the specified aggregate node can be in the same branch with this
	 * aggregate node.
	 *
	 * @param {module:x2node-queries~PropertyTreeNode} otherNode The other
	 * aggregate node.
	 * @returns {boolean} <code>true</code> if compatible.
	 */
	isCompatibleAggregate(otherNode) {

		return (
			(this._aggregateFunc && otherNode._aggregateFunc) &&
				(this._desc.container === otherNode._desc.container) &&
				(
					this._desc.definition.aggregate.collection ===
						otherNode._desc.definition.aggregate.collection
				) &&
				(this._desc.isScalar() && otherNode._desc.isScalar())
		);
	}

	/**
	 * Recursively combine this node with another node and return the new
	 * combined tree. The method does not change this node.
	 *
	 * @param {module:x2node-queries~PropertyTreeNode} [otherNode] The other
	 * node. If not provided, the (sub)tree is cloned without combining with
	 * anything.
	 * @returns {module:x2node-queries~PropertyTreeNode} New combined tree.
	 */
	combine(otherNode) {

		const combined = this._cloneWithoutChildren();

		if (otherNode) {
			if (otherNode._path !== combined._path)
				throw new Error(
					'Internal X2 error: combining nodes with different' +
						' property paths.');
			otherNode._clauses.forEach(clause => {
				combined.addClause(clause);
			});
		}

		const otherChildren = (otherNode && otherNode._children);
		if (this._children || otherChildren) {

			this._children.forEach((childNode, childPropName) => {
				combined.addChildNode(childNode.combine(
					otherChildren && otherChildren.get(childPropName)));
			});

			if (otherChildren)
				otherChildren.forEach((childNode, childPropName) => {
					if (!combined._children.has(childPropName))
						combined.addChildNode(childNode.combine(null));
				});
		}

		return combined;
	}

	/**
	 * Clone this node without carrying over its children.
	 *
	 * @private
	 * @returns {module:x2node-queries~PropertyTreeNode} The node clone.
	 */
	_cloneWithoutChildren() {

		const clone = new PropertyTreeNode(
			this._recordTypes, this._path, this._desc, this._containerChain,
			new Set(this._clauses));

		clone._valueExpr = this._valueExpr;
		clone._aggregateFunc = this._aggregateFunc;
		clone._presenceTest = this._presenceTest;
		clone._filter = this._filter;
		clone._order = this._order;

		return clone;
	}

	/**
	 * Tell if the tree <em>below</em> this node includes a node for the
	 * specified property path (this node is excluded).
	 *
	 * @param {string} propPath Property path.
	 * @returns {boolean} <code>true</code> if the tree includes the property.
	 */
	includesProp(propPath) { return this._includedProps.has(propPath); }

	/**
	 * Find node that represents the specified property among the
	 * <em>descendants</em> (that is excluding this node) of this node.
	 *
	 * @param {string} propPath Property path.
	 * @returns {module:x2node-queries~PropertyTreeNode} The descendant node, or
	 * <code>undefined</code> if not found.
	 */
	findNode(propPath) { return this._includedProps.get(propPath); }

	/**
	 * Property path associated with the node. The path includes the tree's base
	 * value expression context base path.
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
	 * properties. Otherwise, it's <code>null</code>.
	 *
	 * @type {module:x2node-records~PropertiesContainer}
	 * @readonly
	 */
	get childrenContainer() { return this._childrenContainer; }

	/**
	 * Tell if the node is used in the specified clause.
	 *
	 * @param {string} clause The clause.
	 * @returns {boolean} <code>true</code> if used in the clause.
	 */
	isUsedIn(clause) { return this._clauses.has(clause); }

	/**
	 * Tell if the node is used in the select clause (shortcut for
	 * <code>isUsedIn('select')</code>).
	 *
	 * @returns {boolean} <code>true</code> if used in the select clause.
	 */
	isSelected() { return this.isUsedIn('select'); }

	/**
	 * For a calculated value or aggregate property, the value expression. Can be
	 * present only on a scalar, non-object property. For an aggregate, can be
	 * also on a non-object value map.
	 *
	 * @type {module:x2node-queries~ValueExpression}
	 * @readonly
	 */
	get valueExpr() { return this._valueExpr; }

	/**
	 * For an aggregate property, the aggregation function name.
	 *
	 * @type {string}
	 * @readonly
	 */
	get aggregateFunc() { return this._aggregateFunc; }

	/**
	 * Filter specification used to test the object presence. Can be present only
	 * on an optional scalar object property.
	 *
	 * @type {module:x2node-queries~QueryFilter}
	 * @readonly
	 */
	get presenceTest() { return this._presenceTest; }

	/**
	 * The scoped filter specification. Can be present only on a collection view
	 * property.
	 *
	 * @type {module:x2node-queries~QueryFilter}
	 * @readonly
	 */
	get filter() { return this._filter; }

	/**
	 * The scoped order specification. Can be present only on a collection
	 * property.
	 *
	 * @type {module:x2node-queries~QueryOrder[]}
	 * @readonly
	 */
	get order() { return this._order; }

	/**
	 * Tell if the node has children.
	 *
	 * @returns {boolean} <code>true</code> if has children.
	 */
	hasChildren() { return (this._children && (this._children.size > 0)); }

	/**
	 * Get child node.
	 *
	 * @param {string} propName Child property name.
	 * @returns {module:x2node-queries~PropertyTreeNode} Child node, or
	 * <code>undefined</code> if none.
	 */
	getChild(propName) {

		return this._children.get(propName);
	}

	/**
	 * Iterator for node children.
	 *
	 * @type {external:Iterator.<module:x2node-queries~PropertyTreeNode>}
	 * @readonly
	 */
	get children() {

		return this._children.values();
	}

	/**
	 * Tell if the subtree rooted at this node is expanding (this node is for a
	 * collection property or there are collection properties among its
	 * descendants).
	 *
	 * @returns {boolean} <code>true</code> if expanding.
	 */
	isExpanding() { return this._expanding; }

	/**
	 * Tell if the tree <em>below</em> this node is expanding (has any collection
	 * property descendants).
	 */
	hasExpandingChild() { return this._expandingChild; }
}


/**
 * Build properties tree from a list of property path patterns and debranch it.
 *
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top property in the resulting tree. For example, when the tree is being
 * built for a query that fetches records of a given record type, this is the
 * descriptor of the "records" super-property.
 * @param {string} clause Clause for the tree. Every node in the resulting tree
 * will be marked as used in this clause.
 * @param {module:x2node-queries~ValueExpressionContext} baseValueExprCtx
 * Context for any value expressions used by the properties in the tree. All
 * property nodes in the resulting tree will include this context's base path in
 * their <code>path</code> property.
 * @param {string} [scopePropPath] Optional scope property path, including the
 * base value expression context's base path. If provided, any attempt to add a
 * property to the tree that does not lie on the same collection axis with the
 * scope property will result in an error. Thus, if this argument is provided,
 * the resulting tree is guaranteed to have only a single branch. Note, that
 * collections <em>below</em> the scope property are not allowed either.
 * Therefore, trees built using the same scope property will combine into a tree
 * that will still have only a single branch.
 * @param {external:Iterable.<string>} propPatterns Patterns of properties to
 * include in the tree. The patterns are relative to (that is do not include) the
 * base value expression context's base path.
 * @param {Object} [options] Tree building logic options, if any.
 * @param {boolean} [options.noWildcards] If <code>true</code>, wildcard
 * patterns are not allowed.
 * @param {boolean} [options.noAggregates] If <code>true</code>, aggregate
 * properties are not allowed in the tree.
 * @param {boolean} [options.noCalculated] If <code>true</code>, neither
 * calculated value nor aggregate properties are allowed in the tree.
 * @param {boolean} [options.ignoreScopedOrders] If <code>true</code>, scoped
 * order specifications on any included collection properties are ignored.
 * @param {boolean} [options.noScopedFilters] If <code>true</code>, no collection
 * properties with scoped filters are allowed in the tree.
 * @returns {module:x2node-queries~PropertyTreeNode[]} The resulting properties
 * tree branches. If scope property was provided, there will be always only one
 * branch in the returned array.
 * @throws {module:x2node-common.X2UsageError} If the provided specifications or
 * participating property definitions are invalid.
 */
function buildPropsTreeBranches(
	recordTypes, topPropDesc, clause, baseValueExprCtx, scopePropPath,
	propPatterns, options) {

	// create the branching tree top node
	const topNode = PropertyTreeNode.createTopNode(
		recordTypes, topPropDesc, baseValueExprCtx, clause);

	// add direct patterns
	const valuePropsTrees = new Map();
	const excludedPaths = new Set();
	let wcPatterns = (!(options && options.noWildcards) && new Array());
	for (let propPattern of propPatterns) {
		if (propPattern.startsWith('-'))
			excludedPaths.add(propPattern.substring(1));
		else
			addProperty(
				topNode, scopePropPath, propPattern, clause, options,
				valuePropsTrees, wcPatterns);
	}

	// add wildcard patterns
	while (wcPatterns && (wcPatterns.length > 0)) {
		const wcPatterns2 = new Array();
		wcPatterns.forEach(propPattern => {
			if (!excludedPaths.has(propPattern))
				addProperty(
					topNode, scopePropPath, propPattern, clause, options,
					valuePropsTrees, wcPatterns2);
		});
		wcPatterns = wcPatterns2;
	}

	// add scope if requested
	if (options && options.includeScopeProp && scopePropPath &&
		!topNode.includesProp(scopePropPath))
		addProperty(topNode, null, scopePropPath, clause, {
			noWildcards: true,
			noCalculated: true,
			ignoreScopedOrders: true,
			noScopedFilters: true,
			allowLeafObjects: true
		});

	// de-branch the tree and merge in the value trees
	const assertSingleBranch = (branches, enable) => {
		if (enable && (branches.length > 1))
			throw new Error('Internal X2 error: unexpected multiple branches.');
		return branches;
	};
	const valuePropsTreesArray = Array.from(valuePropsTrees.entries());
	return assertSingleBranch(topNode.debranch().map(
		branch => valuePropsTreesArray.reduce((res, pair) => {
			const valuePropPath = pair[0];
			const valuePropsTree = pair[1];
			if (!res.includesProp(valuePropPath))
				return res;
			return res.combine(valuePropsTree);
		}, branch)
	), scopePropPath);
}

/**
 * Add property to the properties tree.
 *
 * @private
 * @param {module:x2node-queries~PropertyTreeNode} topNode Top node of the
 * properties tree.
 * @param {?string} scopeColPath Path of the scope collection property. If
 * provided, the pattern may only belong to the scope collection property's axis.
 * @param {string} propPattern Property pattern. May be a wildcard pattern if the
 * <code>clause</code> argument is "select".
 * @param {string} clause The query clause where the property is used.
 * @param {external:Map.<string,module:x2node-queries~PropertyTreeNode>} valuePropsTrees
 * Map, to which to add generated value property trees.
 * @param {string[]} wcPatterns Array, to which to add extra patterns resulting
 * in the wildcard pattern expansion. Not used unless the <code>clause</code>
 * argument is "select".
 * @returns {module:x2node-queries~PropertyTreeNode} The leaf node representing
 * the property.
 */
function addProperty(
	topNode, scopePropPath, propPattern, clause, options, valuePropsTrees,
	wcPatterns) {

	// process the pattern parts
	let expandChildren = false;
	const propPatternParts = propPattern.split('.');
	const numParts = propPatternParts.length;
	let parentNode = topNode;
	let patternPrefix = topNode.path, patternSuffix = '';
	for (let i = 0; i < numParts; i++) {
		const propName = propPatternParts[i];
		const parentPropDesc = parentNode.desc;

		// check if wildcard
		if (propName === '*') {

			// check if allowed
			if (!wcPatterns)
				throw new common.X2UsageError(
					'Invalid property path "' + propPattern +
						'": wild cards are not allowed here.');

			// set up the expansion
			expandChildren = true;
			if (i < numParts - 1)
				patternSuffix = '.' + propPatternParts.slice(i + 1).join('.');

			// done looping through pattern parts
			break;
		}

		// get the child node
		let node = parentNode.getChild(propName);

		// create new node if necessary
		if (!node) {

			// create new child node
			node = parentNode.addChild(
				propName, clause, scopePropPath, options, propPattern,
				valuePropsTrees);

		} else { // existing node

			// include the existing node in the clause
			node.addClause(clause);
		}

		// add part to the reconstructed pattern prefix
		patternPrefix += propName + '.';

		// advance down the tree
		parentNode = node;
	}

	// expand selected object
	if (!expandChildren && (parentNode.desc.scalarValueType === 'object')) {

		// set up the expansion
		if (wcPatterns)
			expandChildren = true;
		else if (!(options && options.allowLeafObjects)) // check if allowed
			throw new common.X2UsageError(
				'Invalid property path "' + propPattern +
					'": object properties are not allowed here.');
	}

	// generate expanded patterns
	if (expandChildren) {

		// make sure there is a children container
		if (!parentNode.childrenContainer)
			throw new common.X2UsageError(
				'Invalid property pattern "' + propPattern
					+ '": property ' + parentNode.desc.container.nestedPath +
					parentNode.desc.name + ' of ' +
					String(parentNode.desc.container.recordTypeName) +
					' is not an object nor reference and cannot be used as' +
					' an intermediate property in a path.');

		// generate patterns for all nested properties included by default
		parentNode.childrenContainer.allPropertyNames.forEach(propName => {
			const propDesc = parentNode.childrenContainer.getPropertyDesc(
				propName);
			const propDef = propDesc.definition;
			if (((propDef.fetchByDefault === undefined) &&
					!propDesc.isView() &&
					!propDef.valueExpr &&
					!propDef.aggregate) || propDef.fetchByDefault)
				wcPatterns.push(patternPrefix + propName + patternSuffix);
		});
	}

	// return the leaf node
	return parentNode;
}

// export the builder function
exports.buildPropsTreeBranches = buildPropsTreeBranches;
