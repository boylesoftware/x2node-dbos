'use strict';

const common = require('x2node-common');

const FetchDBO = require('./fetch-dbo.js');
const InsertDBO = require('./insert-dbo.js');
const DeleteDBO = require('./delete-dbo.js');
const UpdateDBO = require('./update-dbo.js');
const Transaction = require('./transaction.js');


/**
 * Database operations (DBO) factory.
 *
 * @memberof module:x2node-dbos
 * @inner
 */
class DBOFactory {

	/**
	 * <strong>Note:</strong> The constructor is not accessible from the client
	 * code. Instances are created using module's
	 * [createDBOFactory()]{@link module:x2node-dbos.createDBOFactory}
	 * function.
	 *
	 * @protected
	 * @param {module:x2node-dbos.DBDriver} dbDriver Database driver to use.
	 * @param {module:x2node-records~RecordTypesLibrary} recordTypes Record types
	 * library to build DBOs against.
	 */
	constructor(dbDriver, recordTypes) {

		this._dbDriver = dbDriver;
		this._recordTypes = recordTypes;
	}

	/**
	 * Record types library associated with the factory.
	 *
	 * @member {module:x2node-records~RecordTypesLibrary}
	 * @readonly
	 */
	get recordTypes() { return this._recordTypes; }

	/**
	 * Build a database <em>fetch</em> operation, which queries the database with
	 * <code>SELECT</code> statements and returns the results. Once built, the
	 * DBO can be executed multiple times with different filter parameters.
	 *
	 * @param {string} recordTypeName Name of the top record type to fetch.
	 * @param {Object} [querySpec] Query specification. If unspecified (same as
	 * an empty object), all records are fetched with all properties that are
	 * fetched by default and the records are returned in no particular order.
	 * @param {Array.<string>} [querySpec.props] Record properties to include. If
	 * unspecified, all properties that are fetched by default are included
	 * (equivalent to <code>['*']</code>). Note, that record id property is
	 * always included. To include super-properties, the super-property name is
	 * included among the patterns starting with a dot. Records are not fetched
	 * if only super-properties are provided. To include records as well, a "*"
	 * can be added.
	 * @param {Array.<Array>} [querySpec.filter] The filter specification. If
	 * unspecified, all records are included.
	 * @param {Array.<string>} [querySpec.order] The records order specification.
	 * If unspecified, the records are returned in no particular order.
	 * @param {Array.<number>} [querySpec.range] The range specification, which
	 * is a two-element array where the first element is the first record index
	 * starting from zero and the second element is the maximum number of records
	 * to return. If the range is not specified, all matching records are
	 * returned.
	 * @returns {module:x2node-dbos~FetchDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If the top record type is
	 * unknown.
	 * @throws {module:x2node-common.X2SyntaxError} If the provided query
	 * specification is invalid.
	 */
	buildFetch(recordTypeName, querySpec) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				`Requested top record type ${recordTypeName} is unknown.`);

		// parse records query specification
		let selectedPropPatterns, selectedSuperProps;
		let filterSpec, orderSpec, rangeSpec;
		if (querySpec) {
			if (querySpec.props) {
				if (!Array.isArray(querySpec.props))
					throw new common.X2UsageError(
						'Properties list is not an array.');
				if (querySpec.props.length === 0)
					selectedPropPatterns = new Array();
				else for (let propPattern of querySpec.props) {
					if (propPattern.startsWith('.')) {
						if (propPattern.indexOf('.', 1) > 0)
							throw new common.X2SyntaxError(
								'Super-property name may not contain dots.');
						if (!selectedSuperProps)
							selectedSuperProps = new Array();
						selectedSuperProps.push(propPattern.substring(1));
					} else {
						if (!selectedPropPatterns)
							selectedPropPatterns = new Array();
						selectedPropPatterns.push(propPattern);
					}
				}
				if (selectedPropPatterns) {
					orderSpec = querySpec.order;
					rangeSpec = querySpec.range;
				}
			} else {
				selectedPropPatterns = [ '*' ];
				orderSpec = querySpec.order;
				rangeSpec = querySpec.range;
			}
			filterSpec = querySpec.filter;
		} else {
			selectedPropPatterns = [ '*' ];
		}

		// build and return the DBO
		return new FetchDBO(
			this._dbDriver, this._recordTypes, recordTypeName,
			selectedPropPatterns, selectedSuperProps, filterSpec, orderSpec,
			rangeSpec);
	}

	/**
	 * Build a database <em>insert</em> operation, which creates new records with
	 * <code>INSERT</code> statements.
	 *
	 * @param {string} recordTypeName Name of the record type to insert.
	 * @param {*} record The record to insert.
	 * @returns {module:x2node-dbos~InsertDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If the provided record data is
	 * invalid or the record type is unknown.
	 */
	buildInsert(recordTypeName, record) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				`Specified record type ${recordTypeName} is unknown.`);

		// build and return the DBO
		return new InsertDBO(
			this._dbDriver, this._recordTypes,
			this._recordTypes.getRecordTypeDesc(recordTypeName), record);
	}

	/**
	 * Build a database <em>delete</em> operarion, which deletes records with
	 * <code>DELETE</code> statements. Once built, the DBO can be executed
	 * multiple times with different filter parameters.
	 *
	 * @param {string} recordTypeName Name of the record type to delete.
	 * @param {Array.<Array>} [filter] The filter specification. If unspecified,
	 * all records of the type are deleted.
	 * @returns {module:x2node-dbos~DeleteDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If record type is unknown or
	 * provided filter specification is invalid.
	 */
	buildDelete(recordTypeName, filterSpec) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				`Specified record type ${recordTypeName} is unknown.`);

		// build and return the DBO
		return new DeleteDBO(
			this._dbDriver, this._recordTypes,
			this._recordTypes.getRecordTypeDesc(recordTypeName), filterSpec);
	}

	/**
	 * Build a database <em>update</em> operarion, which updates records with
	 * <code>UPDATE</code> (and potentially <code>INSERT</code> and
	 * <code>DELETE</code> for collection properties) statements. Once built, the
	 * DBO can be executed multiple times with different filter parameters.
	 *
	 * <p><strong>Note:</strong> The DBO is not intended for bulk updates that
	 * include large numbers of records. It loads and locks all matching records
	 * (only the properties it needs) into memory and then updates each record
	 * one by one all within the same transaction. The most common use-case for
	 * the DBO is to update a single record matched by its id.
	 *
	 * <p><strong>Also note</strong> how the "test" patch operation is handled:
	 * When a "test" operation fails for a record, no changes for that record are
	 * made in the database. This does not affect other records in the matched
	 * set, neither it affects the transaction. Whether there were any records
	 * that were not patched because of a failed "test" operation is reported
	 * back in the operation result object (see the <code>testFailed</code>
	 * flag and <code>failedRecordIds</code> array).
	 *
	 * @param {string} recordTypeName Name of the record type to update.
	 * @param {Array.<Object>} patch Patch specification as described by the
	 * [RFC 6902]{@link https://tools.ietf.org/html/rfc6902}.
	 * @param {Array.<Array>} [filter] The filter specification. If unspecified,
	 * all records of the type are updated.
	 * @returns {module:x2node-dbos~UpdateDBO} The DBO object.
	 * @throws {module:x2node-common.X2UsageError} If record type is unknown or
	 * provided filter is invalid.
	 * @throws {module:x2node-common.X2SyntaxError} If the provided patch
	 * specification is invalid.
	 */
	buildUpdate(recordTypeName, patch, filterSpec) {

		// check top record type existense
		if (!this._recordTypes.hasRecordType(recordTypeName))
			throw new common.X2UsageError(
				`Specified record type ${recordTypeName} is unknown.`);

		// build and return the DBO
		return new UpdateDBO(
			this._dbDriver, this._recordTypes,
			this._recordTypes.getRecordTypeDesc(recordTypeName), patch,
			filterSpec);
	}

	/**
	 * Get new transaction handler. The transaction has not been started.
	 *
	 * @param {*} connection The database connection compatible with the database
	 * driver.
	 * @returns {module:x2node-dbos~Transaction} The transaction handler.
	 */
	newTransaction(connection) {

		return new Transaction(this._dbDriver, connection);
	}
}

// export the class
module.exports = DBOFactory;
