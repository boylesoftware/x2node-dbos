'use strict';

const common = require('x2node-common');


/**
 * Super-type symbols registry.
 *
 * @private
 * @type {Object.<string,external:Symbol>}
 */
const SUPERTYPE_SYMBOLS = {};

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
	 * @param {string} topRecordTypeName Name of the query top record type.
	 * @param {Object} querySpec Raw query specification parameters.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid or the top record type is unknown.
	 */
	constructor(recordTypes, topRecordTypeName, querySpec) {

		this._recordTypes = recordTypes;
		this._topRecordTypeName = topRecordTypeName;

		// get top record type
		if (!recordTypes.hasRecordType(topRecordTypeName))
			throw new common.X2UsageError(
				'Requested top record type ' + topRecordTypeName +
					' is unknown.');
		const topRecordTypeDesc = recordTypes.getRecordTypeDesc(
			topRecordTypeName);

		// create super record type if does not yet exist
		let superTypeName = SUPERTYPE_SYMBOLS[topRecordTypeName];
		if (!superTypeName)
			superTypeName = SUPERTYPE_SYMBOLS[topRecordTypeName] = Symbol(
				'$' + topRecordTypeName);
		if (!recordTypes.hasRecordType(superTypeName)) {
			const recordsPropDef = Object.create(topRecordTypeDesc.definition);
			recordsPropDef.valueType = '[object]';
			recordsPropDef.optional = false;
			recordsPropDef.parentIdColumn = '\'' + topRecordTypeName + '\'';
			const superTypeDef = {
				table: '$RecordTypes', // non-existent table
				properties: {
					'name': {
						valueType: 'string',
						role: 'id',
						column: '\'' + topRecordTypeName + '\''
					},
					'records': recordsPropDef
				}
			};
			// TODO: add super-aggregates
			recordTypes.addRecordType(superTypeName, superTypeDef);
		}

		// build selected properties tree
		this._selectedProps = this._buildSelectedProps(
			recordTypes.getRecordTypeDesc(superTypeName),
			topRecordTypeDesc,
			querySpec.props
		);

		// TODO: filter, order and range
		//...
	}

	/**
	 * Build selected properties tree.
	 *
	 * @private
	 * TODO: document arguments and return value
	 */
	_buildSelectedProps(superTypeDesc, topRecordTypeDesc, propPatterns) {

		// start building selected properties tree
		const selectedProps = new Map();

		// add top collection to the tree
		const topRecordsNode = {
			desc: superTypeDesc.getPropertyDesc('records'),
			childrenContainer: topRecordTypeDesc,
			children: new Map()
		};
		selectedProps.set('records', topRecordsNode);

		// add top record id property, which is always selected
		/*topRecordsNode.children.set(topRecordTypeDesc.idPropertyName, {
			desc: topRecordTypeDesc.getPropertyDesc(
				topRecordTypeDesc.idPropertyName)
		});*/

		// process direct patterns
		const excludedPaths = new Map();
		let wcPatterns = new Array();
		(propPatterns ? propPatterns : [ '*' ]).forEach(propPattern => {

			// collect exclusion pattern
			if (propPattern.startsWith('-')) {
				excludedPaths.set(propPattern.substring(1), true);
				return;
			}

			// process super-aggregate
			if (propPattern.startsWith('.')) {
				// TODO: process super-aggregate
				return;
			}

			// process regular inclusion pattern
			this._parseInclusionPattern(topRecordsNode, propPattern, wcPatterns);

		});

		// process wildcard patterns
		while (wcPatterns.length > 0) {
			const wcPatterns2 = new Array();
			wcPatterns.forEach(propPattern => {
				if (!excludedPaths.has(propPattern))
					this._parseInclusionPattern(
						topRecordsNode, propPattern, wcPatterns2);
			});
			wcPatterns = wcPatterns2;
		}

		// return the result tree
		return selectedProps;
	}

	/**
	 * Parse property inclusion pattern.
	 *
	 * @private
	 * TODO: document arguments
	 */
	_parseInclusionPattern(topRecordsNode, propPattern, wcPatterns) {

		// split the pattern into parts
		const propPatternParts = propPattern.split('.');
		const numParts = propPatternParts.length;

		// process the property nested path and create corresponding nodes
		let parentNode = topRecordsNode;
		let patternPrefix = '';
		for (let i = 0; i < numParts - 1; i++) {
			const propPatternPart = propPatternParts[i];

			// add part to the reconstructed pattern prefix
			patternPrefix += propPatternPart + '.';

			// check if the node is already in the tree and advance if so
			if (parentNode.children.has(propPatternPart)) {
				parentNode = parentNode.children.get(propPatternPart);
				continue;
			}

			// create new tree node
			let node;
			const parentPropDesc = parentNode.desc;
			if (parentPropDesc.isPolymorph()) {

				// create subtype node
				node = {
					isSubtypeNode: true,
					desc: parentPropDesc,
					children: new Map()
				};

				// determine the property children container
				if (parentPropDesc.scalarValueType === 'object') {
					node.childrenContainer =
						parentPropDesc.nestedProperties[propPatternPart];
				} else { // polymorph reference
					node.childrenContainer =
						parentPropDesc.refTargets.find(
							n => (n === propPatternPart));
				}
				if (!node.childrenContainer)
					throw new common.X2UsageError(
						'Invalid property inclusion pattern "' +
							propPattern + '": polymorphic property ' +
							parentPropDesc.container.nestedPath +
							parentPropDesc.name + ' of record type ' +
							parentPropDesc.container.recordTypeName +
							' does not have subtype ' + propPatternPart + '.');

			} else { // parent property is not a polymorph

				// check that the parent container has the property
				const container = parentNode.childrenContainer;
				if (!container.hasProperty(propPatternPart))
					throw new common.X2UsageError(
						'Invalid property inclusion pattern "' +
							propPattern + '": record type ' +
							container.recordTypeName +
							' does not have property ' +
							container.nestedPath + propPatternPart + '.');

				// create node for the property
				node = {
					desc: container.getPropertyDesc(propPatternPart),
					children: new Map()
				};

				// determine the property children container
				switch (node.desc.scalarValueType) {
				case 'object':
					if (!node.desc.isPolymorph()) {
						node.childrenContainer = node.desc.nestedProperties;
					}
					break;
				case 'ref':
					if (!node.desc.isPolymorph()) {
						node.childrenContainer =
							this._recordTypes.getRecordTypeDesc(
								node.desc.refTarget);
					}
					break;
				default:
					throw new common.X2UsageError(
						'Invalid property inclusion pattern "' +
							propPattern + '": property ' +
							container.nestedPath + node.desc.name +
							' of record type ' + container.recordTypeName +
							' is neither a nested object nor a reference' +
							' and cannot be used in a nested property path.');
				}

				// add id property
				/*if (container.idPropertyName)
					parentNode.children.set(container.idPropertyName, {
						desc: container.getPropertyDesc(
							container.idPropertyName)
					});*/
			}

			// add the node to the tree
			parentNode.children.set(propPatternPart, node);

			// advance down the tree
			parentNode = node;
		}

		// process the terminal pattern part
		const termPatternPart = propPatternParts[numParts - 1];
		let includeAllChildren = false;
		if (termPatternPart === '*') {
			if (parentNode.isSubtypeNode)
				throw new common.X2UsageError(
					'Invalid property inclusion pattern "' + propPattern +
						'": cannot have wild card pattern following' +
						' polymorphic property subtype.');
			includeAllChildren = true;
		} else {
			if (parentNode.desc.isPolymorph() && !parentNode.isSubtypeNode)
				throw new common.X2UsageError(
					'Invalid property inclusion pattern "' + propPattern +
						'": pattern may not end with a polymorphic' +
						' property subtype name.');
			const container = parentNode.childrenContainer;
			if (!container.hasProperty(termPatternPart))
				throw new common.X2UsageError(
					'Invalid property inclusion pattern "' + propPattern +
						'": record type ' + container.recordTypeName +
						' does not have property ' +
						container.nestedPath + termPatternPart + '.');
			let node = parentNode.children.get(termPatternPart);
			if (!node) {
				node = {
					desc: container.getPropertyDesc(termPatternPart)
				};
				parentNode.children.set(termPatternPart, node);
			}
			patternPrefix += termPatternPart + '.';
			if (node.desc.scalarValueType === 'object') {
				includeAllChildren = true;
				if (!node.desc.isPolymorph())
					node.childrenContainer = node.desc.nestedProperties;
				node.children = new Map();
			}
			parentNode = node;
		}

		// include all children if requested
		function expandWildcardPattern(container, patternPrefix) {
			container.allPropertyNames.forEach(propName => {
				const propDesc = container.getPropertyDesc(propName);
				const propDef = propDesc.definition;
				if (propDef.fetchByDefault ||
					(
						(propDef.fetchByDefault === undefined) &&
							(
								(propDesc.scalarValueType === 'object') ||
									propDef.column ||
									(
										(propDesc.scalarValueType === 'ref') &&
											propDef.reverseRefProperty
									)
							)
					)) wcPatterns.push(patternPrefix + propName);
			});
		}
		if (includeAllChildren) {
			if (!parentNode.desc.isPolymorph()) {
				expandWildcardPattern(
					parentNode.childrenContainer,
					patternPrefix
				);
			} else if (parentNode.desc.scalarValueType === 'object') {
				for (let subtype in parentNode.desc.nestedProperties)
					expandWildcardPattern(
						parentNode.desc.nestedProperties[subtype],
						patternPrefix + subtype + '.'
					);
			} else { // polymorph ref
				parentNode.desc.refTargets.forEach(subtype => {
					expandWildcardPattern(
						this._recordTypes.getRecordTypeDesc(subtype)
							.nestedProperties,
						patternPrefix + subtype + '.'
					);
				});
			}
		}
	}

	/**
	 * Tree of selected properties. The keys in the map are record property
	 * names, the values are objects that have properties: <code>desc</code> for
	 * the property descriptor and <code>children</code> for the map of selected
	 * nested properties.
	 *
	 * @type {external:Map.<string,Object>}
	 * @readonly
	 */
	get selectedProps() { return this._selectedProps; }

	/**
	 * Record types library.
	 *
	 * @type {module:x2node-records~RecordTypesLibrary}
	 * @readonly
	 */
	get recordTypes() { return this._recordTypes; }

	/**
	 * Name of the query top record type.
	 *
	 * @type {string}
	 * @readonly
	 */
	get topRecordTypeName() { return this._topRecordTypeName; }
}

module.exports = QuerySpec;
