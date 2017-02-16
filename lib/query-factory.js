'use strict';

const Query = require('./query.js');
const ValueExpressionContext = require('./value-expression-context.js');
const QuerySpec = require('./query-spec.js');


/**
 * Super-type symbols registry.
 *
 * @private
 * @type {Object.<string,Symbol>}
 */
const SUPERTYPE_SYMBOLS = {};

/**
 * Query factory.
 *
 * @memberof module:x2node-queries
 * @inner
 */
class QueryFactory {

	/**
	 * <b>The constructor is not accessible from the client code. Instances are
	 * created using module's
	 * [createQueryFactory]{@link module:x2node-queries.createQueryFactory}
	 * function.</b>
	 *
	 * @param {module:x2node-queries.DBDriver} dbDriver Database driver to use.
	 */
	constructor(dbDriver) {

		// lookup the driver
		this._dbDriver = dbDriver;
	}

	/**
	 * Build a query. Once built, the query can be used multiple times.
	 *
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} recordTypeName Name of the top record type to fetch.
	 * @param {Object} [querySpec] Query specification. If unspecified (same as
	 * an empty object), all records are fetched with all properties that are
	 * fetched by default and the records are returned in no particular order.
	 * @param {string[]} [querySpec.props] Record properties to include. If
	 * unspecified, all properties that are fetched by default are included
	 * (equivalent to <code>['*']</code>). Note, that record id property is
	 * always included.
	 * @param {string[]} [querySpec.views] Records collection views to fetch.
	 * Special view "records" fetches the records according to the
	 * <code>props</code>, <code>filter</code>, <code>order</code> and
	 * <code>range</code> specifications. Special view "count" includes the total
	 * count of matched records dropping the <code>range</code> specification.
	 * Other valid views include those defined in the <code>views</code> section
	 * of the record type definition. If the views list is not provided, only the
	 * records are fetched without any super-views (equivalent to
	 * <code>['records']</code>).
	 * @param {Array[]} [querySpec.filter] The filter specification. If
	 * unspecified, all records are included.
	 * @param {string[]} [querySpec.order] The records order specification. If
	 * unspecified, the records are returned in no particular order.
	 * @param {number[]} [querySpec.range] The range specification, which is a
	 * two-element array where the first element is the first record index
	 * starting from zero and the second element is the maximum number of records
	 * to return. If the range is not specified, all matching records are
	 * returned.
	 * @returns {module:x2node-queries~Query} The query object.
	 * @throws {module:x2node-common.X2UsageError} If the provided query
	 * specification is invalid, the top record type is unknown or the provided
	 * record types library is not suitable for the specified query.
	 */
	buildFetch(recordTypes, recordTypeName, querySpec) {

		// get top record type descriptor
		if (!recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				'Requested top record type ' + recordTypeName +
					' is unknown.');
		const recordTypeDesc = recordTypes.getRecordTypeDesc(recordTypeName);

		// parse records query specification
		let recordsQuerySpec, superPropsQuerySpec;
		if (querySpec) {
			if (querySpec.views) {
				querySpec.views.forEach(view => {
					if (view === 'records') {
						recordsQuerySpec = querySpec;
					} else {
						if (!superPropsQuerySpec)
							superPropsQuerySpec = {
								props: [],
								filter: querySpec.filter
							};
						superPropsQuerySpec.props.push(view);
					}
				});
			} else {
				recordsQuerySpec = querySpec;
			}
		} else {
			recordsQuerySpec = {};
		}

		// get super type descriptor
		const superTypeDesc = this._getSuperRecordTypeDesc(
			recordTypes, recordTypeName);
		const recordsColPropDesc = superTypeDesc.getPropertyDesc('records');

		// parse records query specification
		let recordsQuerySpecObj = null;
		if (recordsQuerySpec)
			recordsQuerySpecObj = new QuerySpec(
				recordTypes,
				recordsColPropDesc,
				new ValueExpressionContext('', [ recordTypeDesc ]),
				recordsQuerySpec.props,
				recordsQuerySpec.filter,
				recordsQuerySpec.order,
				recordsQuerySpec.range
			);

		// parse super properties query specification
		let superPropsQuerySpecObj = null;
		if (superPropsQuerySpec)
			superPropsQuerySpecObj = new QuerySpec(
				recordTypes,
				superTypeDesc,
				new ValueExpressionContext('', [ superTypeDesc ]),
				superPropsQuerySpec.props,
				superPropsQuerySpec.filter
			);

		// build and return the query
		return new Query(
			this._dbDriver, recordTypes, recordTypeName, recordsQuerySpecObj,
			superTypeDesc.name, superPropsQuerySpecObj);
	}

	/**
	 * Get super record type descriptor for the specified record type. If super
	 * record type has not been registered yet, create and register it.
	 *
	 * @private
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library.
	 * @param {string} recordTypeName Name of the record type, for which to get
	 * the super record type.
	 * @returns {module:x2node-records~RecordTypeDescriptor} Super record type
	 * descriptor.
	 */
	_getSuperRecordTypeDesc(recordTypes, recordTypeName) {

		let superTypeName = SUPERTYPE_SYMBOLS[recordTypeName];
		if (!superTypeName)
			superTypeName = SUPERTYPE_SYMBOLS[recordTypeName] = Symbol(
				'$' + recordTypeName);

		let superTypeDesc;
		if (recordTypes.hasRecordType(superTypeName)) {
			superTypeDesc = recordTypes.getRecordTypeDesc(superTypeName);
		} else {

			const superTypeDef = {
				properties: {
					'recordTypeName': {
						valueType: 'string',
						role: 'id',
						valueExpr: '\'' + recordTypeName + '\''
					},
					'records': {
						valueType: '[ref(' + recordTypeName + ')]',
						optional: false
					},
					'count': {
						valueType: 'number',
						aggregate: {
							collection: 'records',
							valueExpr: 'id => count'
						}
					}
				}
			};

			const recordTypeDesc = recordTypes.getRecordTypeDesc(recordTypeName);
			const recordTypeDef = recordTypeDesc.definition;
			for (let superPropName in recordTypeDef.superProperties) {
				const superPropDef = recordTypeDef.superProperties[
					superPropName];
				if (superTypeDef.properties[superPropName])
					throw new common.X2UsageError(
						'Invalid record type ' + recordTypeName +
							' definition: super property name "' +
							superPropName + '" is reserved.');
				const superTypePropDef = Object.create(superPropDef);
				superTypeDef.properties[superPropName] = superTypePropDef;
			}

			superTypeDesc =
				recordTypes.addRecordType(superTypeName, superTypeDef);
		}

		return superTypeDesc;
	}
}

module.exports = QueryFactory;
