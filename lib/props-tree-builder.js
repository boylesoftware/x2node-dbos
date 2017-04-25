'use strict';

const common = require('x2node-common');

const ValueExpressionContext = require('./value-expression-context.js');


const PARENT_NODE = Symbol('PARENT_NODE');

/**
 * Properties tree node.
 *
 * @protected
 * @memberof module:x2node-dbos
 * @inner
 */
class PropertyTreeNode {

	/**
	 * Create new node. Not used from outside of the class.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} basePath Path of the first node up in the parent chain
	 * that is for a record type (last element in the provided container chain is
	 * a record type descriptor).
	 * @param {string} propPath Property path including the base value expression
	 * context's base path.
	 * @param {module:x2node-records~PropertyDescriptor} propDesc Descriptor of
	 * the property represented by the node.
	 * @param {Array.<module:x2node-records~PropertiesContainer>} containerChain
	 * Chain of property containers leading to the property. The first element in
	 * the chain is the container corresponding to the property with empty path
	 * (always the same container as the first one in the base value expression
	 * context's container chain). The last element is the container of the node
	 * property's children, or <code>null</code> if the property cannot have
	 * children (not an object, nor a reference, nor simple values collection).
	 * @param {Set.<string>} clauses The initial clauses set.
	 */
	constructor(
		recordTypes, basePath, propPath, propDesc, containerChain, clauses) {

		this._recordTypes = recordTypes;
		this._basePath = basePath;
		this._basePrefix = (basePath.length > 0 ? basePath + '.' : '');
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
	 * @param {module:x2node-dbos~ValueExpressionContext} baseValueExprCtx
	 * Base value expressions context.
	 * @param {string} [clause] Optional clause to associate the node with.
	 * @returns {module:x2node-dbos~PropertyTreeNode} The top tree node.
	 */
	static createTopNode(recordTypes, topPropDesc, baseValueExprCtx, clause) {

		const topNode = new PropertyTreeNode(
			recordTypes, baseValueExprCtx.basePath, baseValueExprCtx.basePath,
			topPropDesc, baseValueExprCtx.containerChain, new Set());

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
	 * @returns {module:x2node-dbos~PropertyTreeNode} The new child node.
	 * @param {Map.<string,module:x2node-dbos~PropertyTreeNode>} valuePropsTrees
	 * Map, to which to add generated value property trees.
	 */
	addChild(
		propName, clause, scopePropPath, options, srcPropPattern,
		valuePropsTrees) {

		// invalid pattern function
		const invalidPropPattern = msg => new common.X2SyntaxError(
			`Invalid property path or pattern "${srcPropPattern}": ${msg}`);

		// make sure the property can have children
		if (!this._childrenContainer)
			throw invalidPropPattern(
				'property ' + this._desc.container.nestedPath +
					this._desc.name + ' of ' +
					String(this._desc.container.recordTypeName) +
					' is not an object nor reference and cannot be used as' +
					' an intermediate element in a path.');

		// may not use children of a calculated property
		if (this._desc.isCalculated())
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
				`must lie on the same collection axis with ${scopePropPath}.`);

		// find the base record type node
		let baseNode = this;
		while (!baseNode._childrenContainer.isRecordType())
			baseNode = baseNode[PARENT_NODE];

		// create the child node
		const childNode = new PropertyTreeNode(
			this._recordTypes,
			baseNode._path,
			propPath,
			propDesc,
			this._containerChain.concat(propDesc.nestedProperties),
			new Set()
		);
		childNode[PARENT_NODE] = this;

		// add the clause to the child node
		childNode.addClause(clause);

		// mark as aggregated if requested in the options
		if (options && options.aggregated)
			childNode._aggregated = true;

		// add the child node to the tree and find the top node
		let topNode;
		this._children.set(propName, childNode);
		for (let n = this; n; n = n[PARENT_NODE]) {
			topNode = n;
			n._includedProps.set(propPath, childNode);
		}

		// check if aggregate property
		if (propDesc.isAggregate()) {

			// check if allowed in the options
			if (options && (options.noCalculated || options.noAggregates))
				throw invalidPropPattern(
					'aggregate properties are not allowed here.');

			// create value tree
			const usedPropPaths = new Set();
			propDesc.valueExpr.usedPropertyPaths.forEach(p => {
				usedPropPaths.add(childNode._basePrefix + p);
			});
			if (propDesc.isMap())
				usedPropPaths.add(
					childNode._basePrefix + propDesc.aggregatedPropPath + '.' +
						propDesc.keyPropertyName);
			if (propDesc.filter)
				propDesc.filter.usedPropertyPaths.forEach(p => {
					usedPropPaths.add(childNode._basePrefix + p);
				});
			valuePropsTrees.set(propPath, buildPropsTreeBranches(
				this._recordTypes, topNode.desc, 'value',
				topNode.getValueExpressionContext(),
				childNode._basePrefix + propDesc.aggregatedPropPath,
				usedPropPaths, {
					noWildcards: true,
					noCalculated: true,
					ignoreScopedOrders: true,
					noScopedFilters: true,
					aggregated: true
				})[0]);
		}

		// check if calculated value property
		else if (propDesc.isCalculated()) {

			// check if allowed in the options
			if (options && options.noCalculated)
				throw invalidPropPattern(
					'calculated value properties are not allowed here.');

			// create the value tree
			const usedPropPaths = new Set();
			propDesc.valueExpr.usedPropertyPaths.forEach(p => {
				usedPropPaths.add(childNode._basePrefix + p);
			});
			valuePropsTrees.set(propPath, buildPropsTreeBranches(
				this._recordTypes, topNode.desc, 'value',
				topNode.getValueExpressionContext(),
				this._path, usedPropPaths, {
					noWildcards: true,
					noCalculated: true,
					ignoreScopedOrders: true,
					noScopedFilters: true
				})[0]);
		}

		// check if has a presence test
		else if (propDesc.presenceTest) {

			// check if asked to ignore
			if (!options || ((
				!options.ignorePresenceTestsOn ||
					!options.ignorePresenceTestsOn.has(propPath)) &&
						!options.ignorePresenceTests)) {

				// create the value tree
				const usedPropPaths = new Set();
				propDesc.presenceTest.usedPropertyPaths.forEach(p => {
					usedPropPaths.add(childNode._basePrefix + p);
				});
				valuePropsTrees.set(propPath, buildPropsTreeBranches(
					this._recordTypes, topNode.desc, 'value',
					topNode.getValueExpressionContext(),
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
			}
		}

		// check has scope filter and/or order
		else if (propDesc.filter || propDesc.order) {

			// used property paths collector
			const usedPropPaths = new Set();

			// check if has scoped filter and asked not to ignore
			if (propDesc.filter && !(
				options && options.ignoreFiltersOn && (
					(propPath === options.ignoreFiltersOn) ||
						options.ignoreFiltersOn.startsWith(propPath + '.')))) {

				// check if allowed in the options
				if (options && options.noScopedFilters)
					throw invalidPropPattern(
						'properties with scoped filters are not allowed here.');

				// collect used properties
				propDesc.filter.usedPropertyPaths.forEach(p => {
					usedPropPaths.add(childNode._basePrefix + p);
				});
			}

			// check if has scoped order and asked not to ignore
			if (propDesc.order && !(options && options.ignoreScopedOrders)) {

				// collect used properties
				propDesc.order.usedPropertyPaths.forEach(p => {
					usedPropPaths.add(childNode._basePrefix + p);
				});
			}

			// create the value tree for the scoped filter and order
			if (usedPropPaths.size > 0)
				valuePropsTrees.set(propPath, buildPropsTreeBranches(
					this._recordTypes, topNode.desc, 'value',
					topNode.getValueExpressionContext(),
					propPath, usedPropPaths, {
						noWildcards: true,
						noCalculated: true,
						ignoreScopedOrders: true,
						noScopedFilters: true,
						ignoreFiltersOn: propPath
					})[0]);
		}

		// update this node's branching flags
		if (childNode._expanding && !this._expandingChild) {
			this._expandingChild = childNode;
			for (let n = this; n; n = n[PARENT_NODE])
				n._expanding = true;
		}
		if (propDesc.isAggregate() && !this._hasAggregates)
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
	 * @returns {Array.<module:x2node-dbos~PropertyTreeNode>} The array
	 * containing the generated branches.
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
			this._children.forEach(childNode => {
				if (childNode._desc.isAggregate() || childNode._hasAggregates) {
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
	 * @param {module:x2node-dbos~PropertyTreeNode} childNode The child node.
	 */
	addChildNode(childNode) {

		childNode[PARENT_NODE] = this;

		if (childNode._includedProps)
			childNode._includedProps.forEach((n, p) => {
				this._includedProps.set(p, n);
			});
		this._includedProps.set(childNode._path, childNode);

		this._children.set(childNode._desc.name, childNode);

		if (!childNode._aggregated && childNode._expanding &&
			!this._expandingChild) {
			this._expandingChild = childNode;
			for (let n = this; n; n = n[PARENT_NODE])
				n._expanding = true;
		}

		if (childNode._desc.isAggregate() || childNode._hasAggregates)
			for (let n = this; n; n = n[PARENT_NODE])
				n._hasAggregates = true;
	}

	/**
	 * Tell if the specified aggregate node can be in the same branch with this
	 * aggregate node.
	 *
	 * @param {module:x2node-dbos~PropertyTreeNode} otherNode The other
	 * aggregate node.
	 * @returns {boolean} <code>true</code> if compatible.
	 */
	isCompatibleAggregate(otherNode) {

		return (
			(this._desc.isAggregate() && otherNode._desc.isAggregate()) &&
				(this._desc.container === otherNode._desc.container) &&
				(
					this._desc.aggregatedPropPath ===
						otherNode._desc.aggregatedPropPath
				) &&
				(this._desc.isScalar() && otherNode._desc.isScalar()) &&
				// TODO: optimize: compare if identical filters
				(!this._desc.filter && !otherNode._desc.filter)
		);
	}

	/**
	 * Recursively combine this node with another node and return the new
	 * combined tree. The method does not change this node.
	 *
	 * @param {module:x2node-dbos~PropertyTreeNode} [otherNode] The other
	 * node. If not provided, the (sub)tree is cloned without combining with
	 * anything.
	 * @returns {module:x2node-dbos~PropertyTreeNode} New combined tree.
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
	 * @returns {module:x2node-dbos~PropertyTreeNode} The node clone.
	 */
	_cloneWithoutChildren() {

		const cloned = new PropertyTreeNode(
			this._recordTypes, this._basePath, this._path, this._desc,
			this._containerChain, new Set(this._clauses));

		cloned._aggregated = this._aggregated;

		return cloned;
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
	 * @returns {module:x2node-dbos~PropertyTreeNode} The descendant node, or
	 * <code>undefined</code> if not found.
	 */
	findNode(propPath) { return this._includedProps.get(propPath); }

	/**
	 * Path of the closest record type node up the chain, including this node.
	 *
	 * @member {string}
	 * @readonly
	 */
	get basePath() { return this._basePath; }

	/**
	 * Base path prefix, which is the <code>basePath<code> property followed by a
	 * dot, or empty string.
	 *
	 * @member {string}
	 * @readonly
	 */
	get basePrefix() { return this._basePrefix; }

	/**
	 * Property path associated with the node. The path includes the tree's base
	 * value expression context base path.
	 *
	 * @member {string}
	 * @readonly
	 */
	get path() { return this._path; }

	/**
	 * Descriptor of the property represented by the node.
	 *
	 * @member {module:x2node-records~PropertyDescriptor}
	 * @readonly
	 */
	get desc() { return this._desc; }

	/**
	 * If the property is an object or a reference, this is container for child
	 * properties. Otherwise, it's <code>null</code>.
	 *
	 * @member {module:x2node-records~PropertiesContainer}
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
	 * Tell if the node has children.
	 *
	 * @returns {boolean} <code>true</code> if has children.
	 */
	hasChildren() { return (this._children && (this._children.size > 0)); }

	/**
	 * Get child node.
	 *
	 * @param {string} propName Child property name.
	 * @returns {module:x2node-dbos~PropertyTreeNode} Child node, or
	 * <code>undefined</code> if none.
	 */
	getChild(propName) {

		return this._children.get(propName);
	}

	/**
	 * Iterator for node children.
	 *
	 * @member {Iterator.<module:x2node-dbos~PropertyTreeNode>}
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

	/**
	 * Get value expression context based at this property node.
	 *
	 * @returns {module:x2node-dbos~ValueExpressionContext} Node's value
	 * expression context.
	 */
	getValueExpressionContext() {

		if (!this._valueExprCtx)
			this._valueExprCtx = new ValueExpressionContext(
				this._path, this._containerChain);

		return this._valueExprCtx;
	}
}


/**
 * Build properties tree from a list of property path patterns and debranch it.
 *
 * @protected
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top property in the resulting tree. For example, when the tree is being
 * built for a query that fetches records of a given record type, this is the
 * descriptor of the "records" super-property.
 * @param {string} clause Clause for the tree. Every node in the resulting tree
 * will be marked as used in this clause.
 * @param {module:x2node-dbos~ValueExpressionContext} baseValueExprCtx
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
 * @param {Iterable.<string>} propPatterns Patterns of properties to include in
 * the tree. The patterns are relative to (that is do not include) the base value
 * expression context's base path.
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
 * @returns {Array.<module:x2node-dbos~PropertyTreeNode>} The resulting
 * properties tree branches. If scope property was provided, there will be always
 * only one branch in the returned array.
 * @throws {module:x2node-common.X2SyntaxError} If the provided specifications or
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
		!topNode.includesProp(scopePropPath)) {
		const scopeOptions = Object.create(options);
		scopeOptions.includeScopeProp = false;
		scopeOptions.noCalculated = true;
		scopeOptions.allowLeafObjects = true;
		addProperty(topNode, null, scopePropPath, clause, scopeOptions);
	}

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
 * Build properties tree for a super-properties query and debranch it.
 *
 * @protected
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~RecordTypeDescriptor} recordTypeDesc Record type
 * descriptor.
 * @param {Iterable.<string>} superPropName Selected super-property names.
 * @returns {Array.<module:x2node-dbos~PropertyTreeNode>} The resulting
 * properties tree branches.
 * @throws {module:x2node-common.X2SyntaxError} If the provided specifications or
 * participating property definitions are invalid.
 */
function buildSuperPropsTreeBranches(
	recordTypes, recordTypeDesc, superPropNames) {

	// create the branching tree top node
	const topNode = PropertyTreeNode.createTopNode(
		recordTypes, {
			isScalar() { return true; },
			isCalculated() { return false; },
			refTarget: recordTypeDesc.superRecordTypeName
		},
		new ValueExpressionContext('', [
			recordTypes.getRecordTypeDesc(recordTypeDesc.superRecordTypeName)
		]),
		'select');

	// add super-properties to the tree
	const valuePropsTrees = new Map();
	for (let superPropName of superPropNames)
		addProperty(
			topNode, null, superPropName, 'select', null, valuePropsTrees);

	// de-branch the tree and merge in the value trees
	const valuePropsTreesArray = Array.from(valuePropsTrees.entries());
	return topNode.debranch().map(
		branch => valuePropsTreesArray.reduce((res, pair) => {
			const valuePropPath = pair[0];
			const valuePropsTree = pair[1];
			if (!res.includesProp(valuePropPath))
				return res;
			return res.combine(valuePropsTree);
		}, branch)
	);
}

/**
 * Build possibly branching properties tree assuming no side-value trees are
 * involved.
 *
 * @protected
 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
 * library.
 * @param {module:x2node-records~PropertyDescriptor} topPropDesc Descriptor of
 * the top property in the resulting tree. For example, when the tree is being
 * built for a query against records of a given record type, this is the
 * descriptor of the "records" super-property.
 * @param {string} clause Clause for the tree. Every node in the resulting tree
 * will be marked as used in this clause.
 * @param {module:x2node-dbos~ValueExpressionContext} baseValueExprCtx
 * Context for any value expressions used by the properties in the tree. All
 * property nodes in the resulting tree will include this context's base path in
 * their <code>path</code> property.
 * @param {Iterable.<string>} propPaths Paths of properties to include in the
 * tree. The paths are relative to (that is do not include) the base value
 * expression context's base path.
 * @returns {module:x2node-dbos~PropertyTreeNode} The resulting properties tree.
 * @throws {module:x2node-common.X2SyntaxError} If the provided paths are invalid.
 */
function buildSimplePropsTree(
	recordTypes, topPropDesc, clause, baseValueExprCtx, propPaths) {

	// create the branching tree top node
	const topNode = PropertyTreeNode.createTopNode(
		recordTypes, topPropDesc, baseValueExprCtx, clause);

	// add properties
	const valuePropsTrees = new Map();
	const options = {
		ignoreScopedOrders: true,
		ignorePresenceTests: (clause !== 'select')
	};
	for (let propPath of propPaths)
		addProperty(topNode, null, propPath, clause, options, valuePropsTrees);

	// make sure there was no value trees
	if (valuePropsTrees.size > 0)
		throw new Error(
			'Internal X2 error: unexpected value trees for ' +
				Array.from(valuePropsTrees.keys()).join(', ') + '.');

	// return the tree
	return topNode;
}

/**
 * Add property to the properties tree.
 *
 * @private
 * @param {module:x2node-dbos~PropertyTreeNode} topNode Top node of the
 * properties tree.
 * @param {?string} scopeColPath Path of the scope collection property. If
 * provided, the pattern may only belong to the scope collection property's axis.
 * @param {string} propPattern Property pattern. May be a wildcard pattern if the
 * <code>clause</code> argument is "select".
 * @param {string} clause The query clause where the property is used.
 * @param {Map.<string,module:x2node-dbos~PropertyTreeNode>} valuePropsTrees
 * Map, to which to add generated value property trees.
 * @param {Array.<string>} wcPatterns Array, to which to add extra patterns
 * resulting in the wildcard pattern expansion. Not used unless the
 * <code>clause</code> argument is "select".
 * @returns {module:x2node-dbos~PropertyTreeNode} The leaf node representing
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

		// check if wildcard
		if (propName === '*') {

			// check if allowed
			if (!wcPatterns)
				throw new common.X2SyntaxError(
					`Invalid property path "${propPattern}":` +
						` wild cards are not allowed here.`);

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
			throw new common.X2SyntaxError(
				`Invalid property path "${propPattern}":` +
					` object properties are not allowed here.`);
	}

	// generate expanded patterns
	if (expandChildren) {

		// make sure there is a children container
		if (!parentNode.childrenContainer)
			throw new common.X2SyntaxError(
				`Invalid property pattern "${propPattern}":` +
					` property ${parentNode.desc.container.nestedPath}` +
					`${parentNode.desc.name} of ` +
					`${String(parentNode.desc.container.recordTypeName)}` +
					` is not an object nor reference and cannot be used as an` +
					` intermediate property in a path.`);

		// generate patterns for all nested properties included by default
		parentNode.childrenContainer.allPropertyNames.forEach(propName => {
			const propDesc = parentNode.childrenContainer.getPropertyDesc(
				propName);
			if (propDesc.fetchByDefault)
				wcPatterns.push(patternPrefix + propName + patternSuffix);
		});
	}

	// return the leaf node
	return parentNode;
}

// export the builder functions
exports.buildPropsTreeBranches = buildPropsTreeBranches;
exports.buildSuperPropsTreeBranches = buildSuperPropsTreeBranches;
exports.buildSimplePropsTree = buildSimplePropsTree;
