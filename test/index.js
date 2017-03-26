'use strict';

const expect = require('chai').expect;

const records = require('x2node-records');
const rsparser = require('x2node-rsparser');

const dbos = require('../index.js');


const VALID_RTL_DEFS = {
	'Record1': {
		properties: {
			'id': {
				valueType: 'number',
				role: 'id'
			},
			'version': {
				valueType: 'number',
				role: 'version'
			},
			'createdOn': {
				valueType: 'datetime',
				role: 'creationTimestamp'
			},
			'createdBy': {
				valueType: 'string',
				role: 'creationActor'
			},
			'modifiedOn': {
				valueType: 'datetime',
				role: 'modificationTimestamp'
			},
			'modifiedBy': {
				valueType: 'string',
				role: 'modificationActor'
			},
			'prop_ssspn': {
				valueType: 'string'
			},
			'prop_ssspc': {
				valueType: 'string',
				column: 'prop_value'
			},
			'prop_ssstn': {
				valueType: 'string',
				table: 'prop_values',
				parentIdColumn: 'parent_id'
			},
			'prop_ssstc': {
				valueType: 'string',
				table: 'prop_values',
				parentIdColumn: 'parent_id',
				column: 'prop_value'
			},
			'prop_srspn': {
				valueType: 'ref(Record2)'
			},
			'prop_srspc': {
				valueType: 'ref(Record2)',
				column: 'prop_value'
			},
			'prop_srstn': {
				valueType: 'ref(Record2)',
				table: 'prop_values',
				parentIdColumn: 'parent_id'
			},
			'prop_srstc': {
				valueType: 'ref(Record2)',
				table: 'prop_values',
				parentIdColumn: 'parent_id',
				column: 'prop_value'
			},
			'prop_sop': {
				valueType: 'object',
				properties: {}
			},
			'prop_sot': {
				valueType: 'object',
				table: 'prop_objects',
				parentIdColumn: 'parent_id',
				properties: {}
			},
		}
	},
	'Record2': {
		table: 'record2s',
		properties: {
			'id': {
				valueType: 'number',
				role: 'id',
				generator: null
			},
		}
	}
};


function testPropDesc(recordTypeDesc, propName, params) {

	const propDesc = recordTypeDesc.getPropertyDesc(propName);
	Object.keys(params).forEach(p => {
		let v;
		if ((typeof propDesc[p]) === 'function') {
			v = propDesc[p]();
		} else {
			v = propDesc[p];
		}
		expect(v, recordTypeDesc.name + '#' + propName + ': desc prop ' + p)
			.to.be.equal(params[p]);
	});
}

describe('x2node-dbos', function() {

	describe('Record Types Library Extension', function() {

		it('should process valid record types library', function() {

			const recordTypes = records.with(rsparser, dbos).buildLibrary(
				VALID_RTL_DEFS);
			expect(recordTypes).to.be.ok;

			const recordTypeDesc = recordTypes.getRecordTypeDesc('Record1');
			expect(recordTypeDesc.table).to.be.equal('Record1');
			expect(recordTypeDesc.getRecordMetaInfoPropName('version')).to.be.equal('version');
			expect(recordTypeDesc.getRecordMetaInfoPropName('creationTimestamp')).to.be.equal('createdOn');
			expect(recordTypeDesc.getRecordMetaInfoPropName('creationActor')).to.be.equal('createdBy');
			expect(recordTypeDesc.getRecordMetaInfoPropName('modificationTimestamp')).to.be.equal('modifiedOn');
			expect(recordTypeDesc.getRecordMetaInfoPropName('modificationActor')).to.be.equal('modifiedBy');

			const params = {
				implicitDependentRef: false,
				fetchByDefault: true,
				column: 'id',
				table: undefined,
				parentIdColumn: undefined,
				isRecordMetaInfo: false,
				recordMetaInfoRole: undefined,
				isModifiable: false,
				isGenerated: true,
				generator: 'auto',
				keyColumn: undefined,
				reverseRefPropertyName: undefined,
				isWeakDependency: undefined,
				valueExpr: undefined,
				isCalculated: false,
				aggregateFunc: undefined,
				isAggregate: false,
				aggregatedPropPath: undefined,
				isFiltered: false,
				filter: undefined,
				presenceTest: undefined,
				isOrdered: false,
				order: undefined
			};
			testPropDesc(recordTypeDesc, 'id', params);
			params.column = 'version';
			params.isRecordMetaInfo = true;
			params.recordMetaInfoRole = 'version';
			params.isGenerated = false;
			params.generator = undefined;
			testPropDesc(recordTypeDesc, 'version', params);
			params.column = 'createdOn';
			params.recordMetaInfoRole = 'creationTimestamp';
			testPropDesc(recordTypeDesc, 'createdOn', params);
			params.column = 'createdBy';
			params.recordMetaInfoRole = 'creationActor';
			testPropDesc(recordTypeDesc, 'createdBy', params);
			params.column = 'modifiedOn';
			params.recordMetaInfoRole = 'modificationTimestamp';
			testPropDesc(recordTypeDesc, 'modifiedOn', params);
			params.column = 'modifiedBy';
			params.recordMetaInfoRole = 'modificationActor';
			testPropDesc(recordTypeDesc, 'modifiedBy', params);
			params.column = 'prop_ssspn';
			params.isRecordMetaInfo = false;
			params.recordMetaInfoRole = undefined;
			params.isModifiable = true;
			testPropDesc(recordTypeDesc, 'prop_ssspn', params);
			params.column = 'prop_value';
			testPropDesc(recordTypeDesc, 'prop_ssspc', params);
			params.table = 'prop_values';
			params.parentIdColumn = 'parent_id';
			params.column = 'prop_ssstn';
			testPropDesc(recordTypeDesc, 'prop_ssstn', params);
			params.column = 'prop_value';
			testPropDesc(recordTypeDesc, 'prop_ssstc', params);
		});
	});
});
