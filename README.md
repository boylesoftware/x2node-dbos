# X2 Framework for Node.js | Database Operations

The _Database Operations_ or _DBOs_ module, being a part of _X2 Framework_ for _Node.js_, allows applications to perform complex operations against SQL databases without dealing with SQL queries, parsing result set rows into hierarchical JSON objects and dealing with database and database driver specifics. It allows applications to focus on the high level data structures and business logic instead. Some of the most notable features of the module include:

* Centering on the concept of _records_, which are well defined JSON documents that support complex data structures including nested objects, arrays and maps (nested objects with arbitrary sets of properties). This allows applications to work with the persistent data as easily as it could be with a document-based database while having a full-featured SQL database in the back-end.

* Mapping of _Record Types_ defined using the means of the [x2node-records](https://www.npmjs.com/package/x2node-records) module to the database tables.

* Automatic construction of SQL queries and statements to support the four basic database operations for records and record collections: search/read, create, update and delete.

* Automatic generation of multiple SQL statements to perform complex database operations when necessary, while still reaching for the best efficiency.

* Support for quering records with multiple nested collection properties.

* Support for fetching referred records of other record types all in a single database operation.

* Support for calculated properties and aggregates.

* Support for result set ranges expressed in records, not rows.

* Automatic support for record meta-data such as record versions (which may be useful to support ETags and data modification conflicts detection).

* Respect for the RDBMS features such as data integrity constraints and transactional data locking.

It's worth noting that the DBOs module is not an ORM and it does not attempt to recreate the concepts pioneered by such _Java_ world frameworks as _Hibernate_. It is not built around the idea of automatic synchronization of state between the application-side objects and the database. It is built around the principles of clarity, efficiency and practicality.

Out of the box, the module supports _MySQL_ (_MariaDB_, _Amazon Aurora_) and _PostgreSQL_ databases. Support for more database engines will be included in the future releases. Custom database driver implementations can be plugged in as well.

## Table of Contents

* [Usage](#usage)
  * [Record Types](#record-types)
  * [The DBO Factory](#the-dbo-factory)
  * [Fetching Records](#fetching-records)
  * [Creating Records](#creating-records)
  * [Updating Records](#updating-records)
  * [Deleting Records](#deleting-records)
* [Record Type Definitions](#record-type-definitions)
  * [Mapping Record Types](#mapping-record-types)
  * [Mapping Stored Record Properties](#mapping-stored-record-properties)
    * [Scalar Properties](#scalar-properties)
    * [Collection Properties](#collection-properties)
  * [Dependent Record References](#dependent-record-references)
  * [Shared Link Tables](#shared-link-tables)
  * [Calculated Properties](#calculated-properties)
  * [Aggregate Properties](#aggregate-properties)
  * [Embedded Objects](#embedded-objects)
  * [Polymorphic Objects](#polymorphic-objects)
  * [Ordered Collections](#ordered-collections)
  * [Array Index Column](#array-index-column)
  * [Filtered Collection Views](#filtered-collection-views)
  * [Record Meta-Info Properties](#record-meta-info-properties)
  * [Super-Properties](#super-properties)
* [Fetch DBO](#fetch-dbo)
  * [Selected Properties Specification](#selected-properties-specification)
  * [Filter Specification](#filter-specification)
  * [Order Specification](#order-specification)
  * [Range Specification](#range-specification)
* [Insert DBO](#insert-dbo)
* [Update DBO](#update-dbo)
* [Delete DBO](#delete-dbo)
* [Transactions](#transactions)
* [Data Sources](#data-sources)
* [Database Drivers](#database-drivers)

## Usage

For the purpose of the DBOs module usage demonstration let's say we have the following self-explanatory schema in a _MySQL_ database:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fname VARCHAR(30) NOT NULL,
    lname VARCHAR(30) NOT NULL
);

CREATE TABLE products (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(30) NOT NULL UNIQUE,
    price DECIMAL(5,2) NOT NULL
);

CREATE TABLE orders (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    account_id INTEGER UNSIGNED NOT NULL,
    placed_on TIMESTAMP(3) DEFAULT 0,
    status VARCHAR(10) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE order_items (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    order_id INTEGER UNSIGNED NOT NULL,
    product_id INTEGER UNSIGNED NOT NULL,
    quantity TINYINT UNSIGNED NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (product_id) REFERENCES products (id),
    UNIQUE (order_id, product_id)
);
```

From the point of view of the application, this schema provides storage for three types of records: accounts, products and orders (with order line items). The DBOs will work with these three types of records and will allow searching them, getting their data, creating, updating and deleting them.

### Record Types

The first step is to define the record types library using X2 Framework's [x2node-records](https://www.npmjs.com/package/x2node-records) module and map the records and their properties to the database tables and columns:

```javascript
// load the framework modules
const records = require('x2node-records');
const dbos = require('x2node-dbos');

// construct our sample record types library with DBOs extensions
const recordTypes = records.with(dbos).buildLibrary({
    recordTypes: {
        'Account': {
            table: 'accounts',
            properties: {
                'id': {
                    valueType: 'number',
                    role: 'id'
                },
                'firstName': {
                    valueType: 'string',
                    column: 'fname'
                },
                'lastName': {
                    valueType: 'string',
                    column: 'lname'
                },
                'orderRefs': {
                    valueType: 'ref(Order)[]',
                    reverseRefProperty: 'accountRef'
                }
            }
        },
        'Product': {
            table: 'products',
            properties: {
                'id': {
                    valueType: 'number',
                    role: 'id'
                },
                'name': {
                    valueType: 'string'
                },
                'price': {
                    valueType: 'number'
                }
            }
        },
        'Order': {
            table: 'orders',
            properties: {
                'id': {
                    valueType: 'number',
                    role: 'id'
                },
                'accountRef': {
                    valueType: 'ref(Account)',
                    column: 'account_id',
                    modifiable: false
                },
                'placedOn': {
                    valueType: 'datetime',
                    column: 'placed_on',
                    modifiable: false
                },
                'status': {
                    valueType: 'string'
                },
                'items': {
                    valueType: 'object[]',
                    table: 'order_items',
                    parentIdColumn: 'order_id',
                    properties: {
                        'id': {
                            valueType: 'number',
                            role: 'id'
                        },
                        'productRef': {
                            valueType: 'ref(Product)',
                            column: 'product_id',
                            modifiable: false
                        },
                        'quantity': {
                            valueType: 'number'
                        }
                    }
                }
            }
        }
    }
});
```

Note how the DBOs module is added to the library as an extention to allow all the additional record type and property definition attributes used to map the database schema.

### The DBO Factory

The next step is to constructo the _DBO Factory_, which is used to create the various DBOs:

```javascript
// create DBO factory against the record types library and the DB driver
const dboFactory = dbos.createDBOFactory(recordTypes, 'mysql');
```

The DBO factory is provided with the record types library and the database driver, so that it knows how to construct database engine-specific SQL. Out-of-the-box the module supports [mysql](https://www.npmjs.com/package/mysql) (and other compatible implementations) and [pg](https://www.npmjs.com/package/pg). Custom driver implementations can be provided to the DBO factory as well.

Normally, a factory is created once by the application when it starts up and is used to construct DBOs throughout the application's lifecycle.

The DBO factory can be used to construct four types of DBOs:

* _Fetch DBO_ for searching and loading records from the database.

* _Insert DBO_ for creating new records.

* _Update DBO_ for patching existing records.

* _Delete DBO_ for deleting records.

Some most basic usage examples for the four follow.

### Fetching Records

To search and load _Order_ records from the database described above, the following code could be used:

```javascript
// build a fetch DBO for selecting 5 recent pending orders for a specific account
const fetchDBO = dboFactory.buildFetch('Order', {

    // select all order record properties
    props: [ '*' ],

    // filter by status and account id passed in as a query parameter when executed
    filter: [
        [ 'status => is', 'PENDING' ],
        [ 'accountRef => is', dbos.param('accountId') ]
    ],

    // order in descending order by order placement timestamp
    order: [
        'placedOn => desc'
    ],

    // select first 5 matched order records (that's right, records, not rows!)
    range: [ 0, 5 ]
});

// configure MySQL database connection and connect
const connection = require('mysql').createConnection({
    host: 'localhost',
    database: 'mydatabase',
    user: 'myuser',
    password: 'mypassword'
});
connection.connect(err => {
    if (err) {
        console.error('connection error');
        throw err;
    }
});

// execute the fetch DBO on the connection and close it before exiting
fetchDBO.execute(connection, null, {
    accountId: 10 // for example we want orders for the account id #10
}).then(
    result => {
        console.log('result:\n' + JSON.stringify(result, null, '  '));
        connection.end();
    },
    err => {
        console.error('error:', err);
        connection.end();
    }
);
```

The `result` object will include a `records` property, which will be an array with all matched order records (up to 5 in our example), and may look something like this:

```json
{
  "recordTypeName": "Order",
  "records": [
    {
      "id": 1,
      "accountRef": "Account#10",
      "placedOn": "2017-02-20T18:32:55.000Z",
      "status": "PENDING",
      "items": [
        {
          "id": 101,
          "productRef": "Product#1",
          "quantity": 1
        },
        {
          "id": 102,
          "productRef": "Product#2",
          "quantity": 10
        }
      ]
    },
    {
      "id": 2,
      ...
    },
    ...
  ]
}
```

Don't mind the `null` passed into the DBO's `execute()` method as the second argument for now. It is for the actor performing the operation (anonymous in this case) and it is explained later.

We could request to fetch only specific record properties and also include referred records such as the _Products_ and the _Accounts_:

```javascript
const fetchDBO = dboFactory.buildFetch('Order', {

    // select only specific properties
    props: [
        '.count',               // include total count of matched orders
        'placedOn',             // only placedOn from the order
        'items.quantity',       // plus quantity for the items
        'items.productRef.*',   // plus product and all product properties
        'accountRef.firstName', // plus account first name
        'accountRef.lastName'   // and the last name
    ],
    ...
});
```

The result then would look something like the following:

```json
{
  "recordTypeName": "Order",
  "count": 12,
  "records": [
    {
      "id": 1,
      "accountRef": "Account#10",
      "placedOn": "2017-02-20T18:32:55.000Z",
      "items": [
        {
          "productRef": "Product#1",
          "quantity": 1
        },
        {
          "productRef": "Product#2",
          "quantity": 10
        }
      ]
    },
    {
      "id": 2,
      ...
    },
    ...
  ],
  "referredRecords": {
    "Account#10": {
      "firstName": "John",
      "lastName": "Silver"
    },
    "Product#1": {
      "id": 1,
      "name": "Rope",
      "price": 9.99
    },
    "Product#2": {
      "id": 2,
      "name": "Nails",
      "price": 4.5
    }
  }
}
```

Note that the record id is always included even though not explicitely selected. Also, intermediate properties in the selected property paths are included.

The `count` is a so called _super-aggregate_, which is automatically added to the records types library. The detailed description of super-aggregates is provided later in this manual. For now, it shows the total number of matched records regardless of the query range, which can be convenient on the client side to drive the search results pagination.

### Creating Records

To create a new order record we could use the following code:

```javascript
// create the DBO and pass new order record template to it
const insertDBO = dboFactory.buildInsert('Order', {
    accountRef: 'Account#10',
    placedOn: (new Date()).toISOString(),
    status: 'PENDING',
    items: [
        {
            productRef: 'Product#1',
            quantity: 1
        },
        {
            productRef: 'Product#2',
            quantity: 10
        }
    ]
});

// execute the DBO
insertDBO.execute(connection, null).then(
    recordId => {
        console.log('new Order id: ' + recordId);
    },
    err => {
        console.error('error:', err);
    }
);
```

In our example the record ids are auto-generated in the database, so we do not include them in the record template. The promise returned by the insert DBO resolves to the new record id.

### Updating Records

Updating records involves special objects called _patches_, which are specifications of what needs to be changed in the matched records. Let's say we want to make the following changes to an order with a specific id:

1. Update quantity on the first order line item.

2. Add a new line item to the order.

3. Change the order status.

The following code can be used to do that:

```javascript
// build the patch specification
const patches = require('x2node-patches');
const patch = patches.build(recordTypes, 'Order', [
    { op: 'replace', path: '/items/0/quantity', value: 2 },
    { op: 'add', path: '/items/-', value: {
        productRef: 'Product#3',
        quantity: 1
    } },
    { op: 'replace', path: '/status', value: 'PROCESSING' }
]);

// create the DBO passing the patch and the order record selector filter
const updateDBO = dboFactory.buildUpdate('Order', patch, [
    [ 'id => is', dbos.param('orderId') ]
]);

// execute the DBO
updateDBO.execute(connection, null, null, {
    orderId: 1
}).then(
    result => {
        console.log('update operation status:\n' + JSON.stringify(result, null, '  '));
    },
    err => {
        console.error('error:', err);
    }
);
```

The example above uses X2 Framework's [x2node-patches](https://www.npmjs.com/package/x2node-rsparser) module to build the patch specification using [JSON Patch](https://tools.ietf.org/html/rfc6902) notation.

The third `null` argument to the DBO's `execute()` method is for an optional record validation function that allows validating records after the patch is applied but before the record is saved into the database. This functionality is described later in this manual.

The `result` object returned by the operation will contain information about what records were updated as well as the updated records data itself. This is also described in detail later.

### Deleting Records

Deleting an order record could be done like this:

```javascript
// build the DBO for the specific order id
const deleteDBO = dboFactory.buildDelete('Order', [
    [ 'id => is', dbos.param('orderId') ]
]);

// execute the DBO and pass the order id to it
deleteDBO.execute(connection, null, {
    orderId: 1
}).then(
    result => {
        console.log('delete operation status:\n' + JSON.stringify(result, null, '  '));
    },
    err => {
        console.error('error:', err);
    }
);
```

The `result` object will tell if any records were matched and actually deleted.

### Debug Logging

Sometimes it is useful to see what SQL statements are run against the database. To turn on the DBOs module's debug logging, which includes that information, the `NODE_DEBUG` environment variable must include word "X2_DBO" (see [Node.js API docs](https://nodejs.org/docs/latest-v4.x/api/util.html#util_util_debuglog_section) for details).

## Record Type Definitions

The DBOs module introduces a number of attributes used in record types library definitions to map records and their properties to the database tables and columns. This mapping allows the DBO factory to construct the SQL queries. The DBOs module itself is a record types library extension and must be added to the library for the extended attributes to get processed:

```javascript
const records = require('x2node-records');
const dbos = require('x2node-dbos');

const recordTypes = records.with(dbos).buildLibrary({
    ...
});
```

### Mapping Record Types

Every record type must have the main table, to which it is mapped. This is the table that has the record id as its primary key and normally stores the bulk of the scalar record properties as its columns. To associate a record type with a table, a `table` attribute is used in the definition:

```javascript
const recordTypes = records.with(dbos).buildLibrary({
    recordTypes: {
        ...
        'Order': {
            table: 'orders',
            properties: {
                ...
            }
        },
        ...
    }
});
```

If `table` attribute is not specified, the record type name is used.

### Mapping Stored Record Properties

The term _stored property_ is used to describe record properties that belong to the record type, are stored together with the records of the type. Later we will see that not all properties defined on a record type are stored properties. For example, some properties may be calculated on the fly when records are fetched from the database and therefore are not stored. Other properties may be stored but belong to a different record type and are imported into another record type. Views supported by the basic [x2node-records](https://www.npmjs.com/package/x2node-records) module are another example of properties that are not stored.

A number of attributes specific to the DBOs module is used to map stored record properties to the tables and columns.

#### Scalar Properties

The simples case of a record property is a scalar property stored in the record type's main table column. To associate such property with the specific column, a `column` property definition attribute is used:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'firstName': {
                valueType: 'string',
                column: 'fname'
            },
            'zip': {
                valueType: 'string',
                optional: true
            }
        }
    },
    ...
}
```

This will map the `firstName` property to the `fname` column of the `accounts` table and the `zip` property to its `zip` column (if `column` attribute is not specified, the property name is used):

```sql
CREATE TABLE accounts (
    ...
    fname VARCHAR(30) NOT NULL,
    zip CHAR(5),
    ...
);
```

The column data type must match the property value type. Also, the column may be nullable if the property is optional.

A property does not have to be stored in the main record type table. If a property is stored in a separate table, the property definition must have a `table` attribute and a `parentIdColumn` attribute, which specifies the column in the property table that points back at the parent record id. Normally it does not make much sense to store scalar, simple value properties in a separate table, but it will work nonetheless. It makes more sense when it comes to nested object properties. For example, if an account address is stored in a separate table in a one-to-one relation with the main record table:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED PRIMARY KEY,
    ...
);

CREATE TABLE account_addresses (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    street VARCHAR(50) NOT NULL,
    ...
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

then it can be mapped like this:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'address': {
                valueType: 'object',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                properties: {
                    'street': {
                        valueType: 'string'
                    }
                    ...
                }
            }
        }
    },
    ...
}
```

#### Collection Properties

When it comes to array and map properties, use of property tables becomes necessary due to the RDBMS nature. The example above can be modified to allow multiple addresses on an account:

```sql
CREATE TABLE account_addresses (
    id INTEGER UNSIGNED PRIMARY KEY,
    account_id INTEGER UNSIGNED NOT NULL,
    street VARCHAR(50) NOT NULL,
    ...
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

and the record type definition:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addresses': {
                valueType: 'object[]',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'street': {
                        valueType: 'string'
                    }
                    ...
                }
            }
        }
    },
    ...
}
```

If an address has a type ("Home", "Work", etc.):

```sql
CREATE TABLE account_addresses (
    id INTEGER UNSIGNED PRIMARY KEY,
    account_id INTEGER UNSIGNED NOT NULL,
    type VARCHAR(10) NOT NULL,
    street VARCHAR(50) NOT NULL,
    ...
    FOREIGN KEY (account_id) REFERENCES accounts (id),
    UNIQUE (account_id, type)
);
```

then we could have the addresses on the account as a map:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addresses': {
                valueType: 'object{}',
                keyPropertyName: 'type',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'type': {
                        valueType: 'string'
                    },
                    'street': {
                        valueType: 'string'
                    }
                    ...
                }
            }
        }
    },
    ...
}
```

If we don't want to have the address type as a property on the address record, then the key column can be specified using `keyColumn` property definition attribute instead of using `keyPropertyName`:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addresses': {
                valueType: 'object{}',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                keyColumn: 'type',
                keyValueType: 'string',
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'street': {
                        valueType: 'string'
                    }
                    ...
                }
            }
        }
    },
    ...
}
```

Note, that in that case the `keyValueType` attribute must be provided as well (see [x2node-rsparser](https://www.npmjs.com/package/x2node-rsparser) module for details).

Simple value collection properties utilize the `column` attribute to map the column in the collection table. For example, if we have a list of phone numbers associated with the account:

```sql
CREATE TABLE account_phones (
    account_id INTEGER UNSIGNED NOT NULL,
    phone CHAR(10) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

the definition will be:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'phones': {
                valueType: 'string[]',
                table: 'account_phones',
                parentIdColumn: 'account_id',
                column: 'phone'
            }
        }
    },
    ...
}
```

And if we have a phone type as a map key:

```sql
CREATE TABLE account_phones (
    account_id INTEGER UNSIGNED NOT NULL,
    type VARCHAR(10) NOT NULL,
    phone CHAR(10) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id),
    UNIQUE (account_id, type)
);
```

the definition will be:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'phones': {
                valueType: 'string{}',
                table: 'account_phones',
                parentIdColumn: 'account_id',
                keyColumn: 'type',
                keyValueType: 'string',
                column: 'phone'
            }
        }
    },
    ...
}
```

Note that use of `keyColumn` attribute becomes the only option for simple value map properties (no `keyPropertyName` is appropriate).

There is an important deviation between how the `keyColumn` and `keyPropertyName` attributes work when it comes to reference property maps. Such maps (as well as arrays) introduce a link table between the main tables of the referrer and the referred record types. When the `keyPropertyName` is used to map the map key, the key is located in the referred record type table. For example, if we extract the address records in the examples above into a separate record type, the tables will be:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED PRIMARY KEY,
    ...
);

CREATE TABLE account_addresses (
    account_id INTEGER UNSIGNED NOT NULL,
    address_id INTEGER UNSIGNED NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id),
    FOREIGN KEY (address_id) REFERENCES addresses (id),
    UNIQUE (account_id, address_id)
);

CREATE TABLE addresses (
    id INTEGER UNSIGNED PRIMARY KEY,
    type VARCHAR(10) NOT NULL,
    street VARCHAR(50) NOT NULL,
    ...
);
```

and the definitions:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addressRefs': {
                valueType: 'ref(Address){}',
                keyPropertyName: 'type',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                column: 'address_id'
            }
        }
    },
    'Address': {
        table: 'addresses',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            'type': {
                valueType: 'string'
            },
            'street': {
                valueType: 'string'
            },
            ...
        }
    },
    ...
}
```

So, the `type` property for the map key is taken from the `addresses` table. On the other hand, if `keyColumn` attribute is used, the column is mapped to the link table and _not_ the referred record type table:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED PRIMARY KEY,
    ...
);

CREATE TABLE account_addresses (
    account_id INTEGER UNSIGNED NOT NULL,
    type VARCHAR(10) NOT NULL,
    address_id INTEGER UNSIGNED NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id),
    FOREIGN KEY (address_id) REFERENCES addresses (id),
    UNIQUE (account_id, type)
);

CREATE TABLE addresses (
    id INTEGER UNSIGNED PRIMARY KEY,
    street VARCHAR(50) NOT NULL,
    ...
);
```

and the definitions:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addressRefs': {
                valueType: 'ref(Address){}',
                table: 'account_addresses',
                parentIdColumn: 'account_id',
                keyColumn: 'type',
                keyValueType: 'string',
                column: 'address_id'
            }
        }
    },
    'Address': {
        table: 'addresses',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            'street': {
                valueType: 'string'
            },
            ...
        }
    },
    ...
}
```

### Dependent Record References

Some reference properties are not stored with the referring record. Instead, there is a reference property in the referred record type that points back at the referring record. The referred record type in this case is called a _dependent_ record type, because its records can exist only in the context of the referring record (unless the reverse reference property is optional), meaning the referring record must exist for the referred record to point at it.

For example, if we have records types _Account_ and _Order_ and the account has a list of references to the orders made for that account. The _Order_, even though a separate record type, has a reference back to the account it belongs to and can only exist in a context of an account. That makes the _Order_ a dependent record type.

Here are the tables:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    ...
);

CREATE TABLE orders (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    account_id INTEGER UNSIGNED NOT NULL,
    ...
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

and the definitions:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'orderRefs': {
                valueType: 'ref(Order)[]',
                reverseRefProperty: 'accountRef'
            }
        }
    },
    'Order': {
        table: 'orders',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            'accountRef': {
                valueType: 'ref(Account)',
                column: 'account_id',
                modifiable: false
            },
            ...
        }
    },
    ...
}
```

The `orderRefs` property of the _Account_ record type is not stored anywhere in any tables and columns associated with it, so it is not a stored property of the _Account_ record type. Instead, it is a property of the dependent _Order_ record type and is stored with the order records in `accountRef` property. This is why the `orderRefs` property does not have any `table` and `column` definition attributes. Instead, it has a `reverseRefProperty` attribute that names the property in the referred record type that points back at the referring record.

The dependent record reference properties are only allowed as top record type properties (not in nested objects). And the reverse reference properties in the referred record types are also only allowed to be top record type properties, plus they are required to be scalar, non-polymorph and not be dependent record references themselves. They are, however, allowed to be stored in a separate table (this may be useful when `keyColumn` is used with a dependent record reference map property, in which case the map key column will reside in the separate link table).

As it will be shown later in this manual, normally, when a record with dependent record reference properties is deleted, the operation is automatically cascaded on to the dependent records, which are deleted as well. This is called _strong dependencies_. In the example above, it makes sense to delete all _Order_ records associated with an _Account_ when the _Account_ record is deleted from the database. Sometimes, however, a dependent reference mechanism needs to be used to import a reference property to a record (for data access convenience), but the referred record type is not exactly dependent. This is called a _weak dependence_. One side effect of a weak dependence is that the deletion operation is not cascaded over it (the operation will fail on the database level if referred records exist and foreign key constraints are properly utilized). To make a dependent reference property weak, a `weakDependency` property definition attribute can be added with value `true`.

### Shared Link Tables

Sometimes, two record types may have references at each other, but none of them is dependent on another. For example, if we have record types _Account_ and _Address_ and they are in a many-to-many relationship so that a single address can be shared by multiple accounts and an account can have multiple addresses:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED PRIMARY KEY,
    ...
);

CREATE TABLE accounts_addresses (
    account_id INTEGER UNSIGNED NOT NULL,
    address_id INTEGER UNSIGNED NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id),
    FOREIGN KEY (address_id) REFERENCES addresses (id),
    UNIQUE (account_id, address_id)
);

CREATE TABLE addresses (
    id INTEGER UNSIGNED PRIMARY KEY,
    ...
);
```

At the same time, we want to have a list of addresses on the account records and we want to have a list of accounts that use an address on the address record. It can be done like this:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'addressRefs': {
                valueType: 'ref(Address)[]',
                table: 'accounts_addresses',
                parentIdColumn: 'account_id',
                column: 'address_id'
            }
        }
    },
    'Address': {
        table: 'addresses',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'accountRefs': {
                valueType: 'ref(Account)[]',
                table: 'accounts_addresses',
                parentIdColumn: 'address_id',
                column: 'account_id'
            }
        }
    },
    ...
}
```

Properties `addressRefs` on record type _Account_ and `accountRefs` on record type _Address_ share the same link table `accounts_addresses`. This case is unique in the sense that modification of a record of one record type may have a side-effect of making changes to stored properties of some records of another record type.

### Calculated Properties

It is possible to have properties on a record type that are not stored in the database directly, but are calculated every time a record is fetched from the database. Such properties are called _calculated properties_. Instead of a column and table, a calculated property has a value expression associated with it using `valueExpr` property definition attribute. The `valueExpr` attribute is a string that uses a special expression syntax that includes value literals, references to other properties of the record, value transformation functions and operators. Here is the expression language EBNF definition:

```ebnf
Expr = [ "+" | "-" ] , Term , { ( "+" | "-" ) , Term }
    | String
    | Boolean
    ;

Term = Factor , { ( "*" | "/" ) , Factor }
    ;

Factor = FunctionCall
    | PropertyRef
    | Number
    | "(" , Expr , ")"
    ;

FunctionCall = FunctionName , "(" , Expr , { "," , Expr } , ")"
    ;

String = '"' , { CHAR } , '"' | "'" , { CHAR } , "'" ;
Boolean = "true" | "false" ;
Number = DIGIT , { DIGIT } , [ "." , DIGIT , { DIGIT } ] ;
FunctionName = ? See Description ? ;
PropertyRef = ? See Description ? ;
```

The following value transformation functions are supported:

* `length`, `len` - Takes single string argument, yields string length.

* `lower`, `lc`, `lcase`, `lowercase` - Takes single string argument, yields all lower-case string.

* `upper`, `uc`, `ucase`, `uppercase` - Takes single string argument, yields all upper-case string.

* `substring`, `sub`, `mid`, `substr` - Yields substring of the string provided as the first argument. The second argument is the first substring character index, starting from zero. The optional third argument is the maximum substring length. If not provided, the end of the input string is used.

* `lpad` - Pads the string provided as the first argument on the left to achieve the minimum length provided as the second argument using the character provided as the third argument as the padding character.

* `concat`, `cat` - Concatenate provided string arguments.

* `coalesce` - Yield first non-null argument.

The properties in the expressions are referred using dot notation. In a calculated nested object property, to refer to a property in the parent object a caret character is used to denote the immediate parent.

For example, given the record types library used in the opening [Usage](#usage) section, the _Order_ records could be augmented with some calculated properties:

```javascript
{
    ...
    'Order': {
        ...
        properties: {
            ...
            'customerName': {
                valueType: 'string',
                valueExpr: 'concat(accountRef.firstName, " ", accountRef.lastName)'
            },
            ...
            'items': {
                ...
                properties: {
                    ...
                    'totalCost': {
                        valueType: 'number',
                        valueExpr: 'productRef.price * quantity'
                    },
                    'orderStatus': {
                        valueType: 'string',
                        valueExpr: '^.status'
                    }
                }
            }
        }
    },
    ...
}
```

This example shows that reference properties can be hopped over to access properties of the referred records. Also, if there were more nesting levels, jumping over multiple children (`prop.nestedProp.moreNestedProp`) and multiple parents (`^.^.grandparentProp.nestedProp`) is possible with the dot notation.

When the records with calculated properties are fetched from the database, the expressions are converted into SQL value expressions and are calculated on the database side.

### Aggregate Properties

A special case of a calculated property is an _aggregate property_. An aggregate property requires:

* A collection property, elements of which it aggregates. The collection is called _aggregated collection_.
* An expression for the aggregated values, called _aggregated value expression_.

* The _aggregation function_, which determines how the values are aggregated.

* And optionally a filter to include only certain elements of the aggregated collection.

An `aggregate` property definition attribute is used to specify these elements. For example, our _Order_ records could include properties that aggregate the order line items like the following:

```javascript
{
    ...
    'Order': {
        table: 'orders',
        properties: {
            ...
            'itemsCount': {
                valueType: 'number',
                aggregate: {
                    collection: 'items',
                    valueExpr: 'id => count'
                }
            },
            'orderTotal': {
                valueType: 'number',
                aggregate: {
                    collection: 'items',
                    valueExpr: 'productRef.price * quantity => sum'
                }
            },
            'expensiveProductsCount': {
                valueType: 'number',
                aggregate: {
                    collection: 'items',
                    valueExpr: 'quantity => sum',
                    filter: [
                        [ 'productRef.price => min', 1000 ]
                    ]
                }
            },
            ...
            'items': {
                valueType: 'object[]',
                table: 'order_items',
                parentIdColumn: 'order_id',
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'productRef': {
                        valueType: 'ref(Product)',
                        column: 'product_id',
                        modifiable: false
                    },
                    'quantity': {
                        valueType: 'number'
                    }
                }
            }
        }
    },
    ...
}
```

The value expressions for the aggregate properties have format:

```
Expr => AggregateFunc
```

Where `Expr` is a regular value expression calculated in the context of the aggregated collection property, and `AggregateFunc` is one of:

* `count` - Number of unique values yielded by the aggregated expression.

* `sum` - Sum of the values yielded by the aggregated expression.

* `min` - The smallest value yielded by the aggregated expression.

* `max` - The largest value yielded by the aggregated expression.

* `avg` - Average of the values yielded by the aggregated expression.

The optional filter specification format will be discussed later in this manual when we talk about fetch DBO.

### Embedded Objects

Properties of a scalar nested object do not have to be stored in a separate table. The nested object's properties can be stored in the columns of the main record type table and the nested object can be used simply to organize the JSON structure of the record. For example, an address on an _Account_ record could be stored in the same table:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fname VARCHAR(30) NOT NULL,
    lname VARCHAR(30) NOT NULL,
    street VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state CHAR(2) NOT NULL,
    zip CHAR(5) NOT NULL
);
```

but be a nested object in the record type:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            'firstName': {
                valueType: 'string',
                column: 'fname'
            },
            'lastName': {
                valueType: 'string',
                column: 'lname'
            },
            'address': {
                valueType: 'object',
                properties: {
                    'street': {
                        valueType: 'string'
                    },
                    'city': {
                        valueType: 'string'
                    },
                    'state': {
                        valueType: 'string'
                    },
                    'zip': {
                        valueType: 'string'
                    }
                }
            }
        }
    },
    ...
}
```

Since the `address` property does not have a `table` attribute, the framework knows that it is stored in the parent table.

In the example above, the `address` property is required. Things turn slightly more complicated if the property needs to be optional. First, the `NOT NULL` constraints are removed from the table:

```sql
CREATE TABLE accounts (
    ...
    street VARCHAR(50),
    city VARCHAR(50),
    state CHAR(2),
    zip CHAR(5)
);
```

But now, the framework needs to be able to tell if the `address` nested object is present on a record it's loading from the database or not. To do that, the `presenceTest` attribute is specified on the property definition. The presence test is a Boolean expression written as a filter specification, which is discussed in detail later in this manual. But here is our example:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'address': {
                valueType: 'object',
                optional: true, // address is now optional
                presenceTest: [
                    [ 'state => present' ],
                    [ 'zip => present' ]
                ],
                properties: {
                    'street': {
                        valueType: 'string',
                        optional: true
                    },
                    'city': {
                        valueType: 'string',
                        optional: true
                    },
                    'state': {
                        valueType: 'string'
                    },
                    'zip': {
                        valueType: 'string'
                    }
                }
            }
        }
    },
    ...
}
```

This definition makes properties `street` and `city` optional, but `state` and `zip` are still required. The presence test checks if `state` and `zip` are present on the record, and if they are not, the whole `address` property is considered to be absent.

### Polymorphic Objects

Polymorphic objects are treated very similarly to nested objects where each polymorphic object subtype is like a nested object property on the base polymorphic object with the properties that describe the subtype-specific properties. The table mappings work the same as for scalar nested objects. For example, if we have two types of payment information that can be associated with an account&mdash;credit cards and bank accounts&mdash;the tables could be:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fname VARCHAR(30) NOT NULL,
    lname VARCHAR(30) NOT NULL
);

CREATE TABLE account_creditcards (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    association VARCHAR(20) NOT NULL,
    last4digits CHAR(4) NOT NULL,
    expdate CHAR(4) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE account_bankaccounts (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    routing_num CHAR(9) NOT NULL,
    account_type VARCHAR(10) NOT NULL,
    last4digits CHAR(4) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);
```

then the record type definition would be:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'paymentInfo': {
                valueType: 'object',
                typePropertyName: 'type',
                subtypes: {
                    'CREDIT_CARD': {
                        table: 'account_creditcards',
                        parentIdColumn: 'account_id',
                        properties: {
                            'association': {
                                valueType: 'string'
                            },
                            'last4Digits': {
                                valueType: 'string',
                                column: 'last4digits'
                            },
                            'expirationDate': {
                                valueType: 'string',
                                column: 'expdate'
                            }
                        }
                    },
                    'BANK_ACCOUNT': {
                        table: 'account_bankaccounts',
                        parentIdColumn: 'account_id',
                        properties: {
                            'routingNumber': {
                                valueType: 'string',
                                column: 'routing_num'
                            },
                            'accountType': {
                                valueType: 'string',
                                column: 'account_type'
                            },
                            'last4Digits': {
                                valueType: 'string',
                                column: 'last4digits'
                            }
                        }
                    }
                }
            }
        }
    },
    ...
}
```

Properties common to all subtypes could be stored either in the parent table (in the example above the `last4digits` column is moved to the `accounts` table and the `last4Digits` property is moved to the `properties` section of the `paymentInfo` property definition), or they could be stored in a separate table:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    fname VARCHAR(30) NOT NULL,
    lname VARCHAR(30) NOT NULL
);

CREATE TABLE account_paymentinfos (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    last4digits CHAR(4) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE account_creditcards (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    association VARCHAR(20) NOT NULL,
    expdate CHAR(4) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account_paymentinfos (id)
);

CREATE TABLE account_bankaccounts (
    account_id INTEGER UNSIGNED NOT NULL UNIQUE,
    routing_num CHAR(9) NOT NULL,
    account_type VARCHAR(10) NOT NULL,
    FOREIGN KEY (account_id) REFERENCES account_paymentinfos (id)
);
```

and the definitions:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'paymentInfo': {
                valueType: 'object',
                typePropertyName: 'type',
                table: 'account_paymentinfos',
                parentIdColumn: 'account_id',
                properties: {
                    'last4Digits': {
                        valueType: 'string',
                        column: 'last4digits'
                    }
                },
                subtypes: {
                    'CREDIT_CARD': {
                        table: 'account_creditcards',
                        parentIdColumn: 'account_id',
                        properties: {
                            'association': {
                                valueType: 'string'
                            },
                            'expirationDate': {
                                valueType: 'string',
                                column: 'expdate'
                            }
                        }
                    },
                    'BANK_ACCOUNT': {
                        table: 'account_bankaccounts',
                        parentIdColumn: 'account_id',
                        properties: {
                            'routingNumber': {
                                valueType: 'string',
                                column: 'routing_num'
                            },
                            'accountType': {
                                valueType: 'string',
                                column: 'account_type'
                            }
                        }
                    }
                }
            }
        }
    },
    ...
}
```

When the framework builds queries for fetching account records in the examples above, it will join all the subtype tables and see which one has a record. Depending on that, it will figure out the nested object subtype. That logic relies on the database having a single record in only a single subtype table for a given base polymorphic record. In the second example, it is enforced by having a `UNIQUE` constraint on the `account_id` column of the `account_paymentinfos` table. In the first example, however, it is not completely enforced on the database level&mdash;physically it is possible to have a row in both `account_creditcards` and `account_bankaccounts` tables each sharing the same account id.

Sometimes, a subtype does not have any subtype-specific properties and therefore there is no need for a separate table for it. In that case, the framework cannot determine the subtype by joining the tables and simply checking which one has a record. To solve it, the type property needs to be actually stored in a column on the base polymorphic object's table. For example, if we have a polymorphic _Event_ record type where subtype _OPENED_ has a subtype-specific property `openerName`, which, for the example sake, is stored in a separate table, and subtype _CLOSED_ does not have any subtype-specific properties. The tables are:

```sql
CREATE TABLE events (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    type VARCHAR(10) NOT NULL,
    happened_on TIMESTAMP
);

CREATE TABLE events_open (
    id INTEGER UNSIGNED NOT NULL UNIQUE,
    opener_name VARCHAR(50) NOT NULL,
    FOREIGN KEY (id) REFERENCES events (id)
);
```

The `type` column of the `events` table is used to store the event type (either "OPENED" or "CLOSED"). The definition should then be:

```javascript
{
    ...
    'Event': {
        table: 'events',
        typePropertyName: 'type',
        typeColumn: 'type',
        properties: {
            'happenedOn': {
                valueType: 'datetime',
                column: 'happened_on'
            }
        },
        subtypes: {
            'OPENED': {
                table: 'events_open',
                parentIdColumn: 'id',
                properties: {
                    'openerName': {
                        valueType: 'string',
                        column: 'opener_name'
                    }
                }
            },
            'CLOSED': {
                properties: {}
            }
        }
    },
    ...
}
```

Note the `typeColumn` attribute that tells the framework where to find/store the record subtype.

### Ordered Collections

When collection properties are fetched from the database, it is possible to specify the order in which they are returned. For that, any collection property definition can have an `order` attribute. For example:

```javascript
{
    ...
    'Order': {
        table: 'orders',
        properties: {
            ...
            'items': {
                valueType: 'object[]',
                table: 'order_items',
                parentIdColumn: 'order_id',
                order: [ 'productRef.name', 'quantity => desc' ],
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'productRef': {
                        valueType: 'ref(Product)',
                        column: 'product_id',
                        modifiable: false
                    },
                    'quantity': {
                        valueType: 'number'
                    }
                }
            }
        }
    },
    ...
}
```

Or, another example:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'orderRefs': {
                valueType: 'ref(Order)[]',
                reverseRefProperty: 'accountRef',
                order: [ 'placedOn => desc' ]
            }
        }
    },
    ...
}
```

The order specification is described in detail later in this manual when we talk about the fetch DBO.

Note, that to order a simple value collection by the value, `$value` pseudo-property reference can be used.

### Array Index Column

_Will be implemented in a (near) future release._

### Filtered Collection Views

It is also possible to specify filtered views of collection properties that include only those elements that match a certain criteria. For example:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
        properties: {
            ...
            'orderRefs': {
                valueType: 'ref(Order)[]',
                reverseRefProperty: 'accountRef'
            },
            'pendingOrderRefs': {
                viewOf: 'orderRefs',
                filter: [
                    [ 'status => is', 'PENDING' ]
                ]
            }
        }
    },
    ...
}
```

Note that the `filter` attribute can be specified only on a view property, not on the actual property itself, which always includes all of its elements.

The filter specification is described in detail later in this manual when we talk about the fetch DBO.

### Record Meta-Info Properties

The DBOs module introduces a number special _meta-info properties_ that can be specified on a record type and are maintained automatically by the framework. Such properties use `role` property definition attribute to specify their type. For example:

```javascript
{
    ...
    'Account': {
        table: 'accounts',
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
                role: 'creationTimestamp',
                column: 'created_on'
            },
            'createdBy': {
                valueType: 'string',
                role: 'creationActor',
                column: 'created_by'
            },
            'modifiedOn': {
                valueType: 'datetime',
                role: 'modificationTimestamp',
                column: 'modified_on'
            },
            'modifiedBy': {
                valueType: 'string',
                role: 'modificationActor',
                column: 'modified_by'
            },
            ...
        }
    },
    ...
}
```

Which has to be reflected in the `accounts` database table:

```sql
CREATE TABLE accounts (
    id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    version INTEGER UNSIGNED NOT NULL,
    created_on TIMESTAMP(3) DEFAULT 0,
    created_by VARCHAR(30) NOT NULL,
    modified_on TIMESTAMP(3) NULL,
    modified_by VARCHAR(30),
    ...
);
```

The following meta-info property roles are supported:

* `version` - Record version. A new record will have version 1. Every time a record is updated, the framework will increment the version property.

* `creationTimestamp` - Timestamp when the record was created.

* `creationActor` - Stamp of the actor that created the record (see [x2node-common](https://www.npmjs.com/package/x2node-common) module for the framework's notion of _actor_).

* `modificationTimestamp` - Timestamp when the record was last time modified. The property is optional by default.

* `modificationActor` - Stamp of the actor that modified the record last time. The property is optional by default.

All meta-info properties are marked as non-modifiable and from the application point of view they are read only.

### Super-Properties

Simetimes information needs to be fetched from the database that is not a specific record property. Instead, this may be information about a collection of records that match the query's criteria. Mostly, this is about aggregate values, such as the total count of matched records, or a sum of all _Order_ amounts, etc. This functionality is supported via so-called _super-properties_ or, if they are aggregates, _super-aggregates_. Super-aggregates can be added to a query specification to be included in the result.

Every record type automatically defines a super-aggregate called `count`, which reflects the number of records in the matched record set. It is possible to add custom super-aggregates as well. For example, if we want to be able to query the total amount for orders matching a certain criteria (or all orders available in the database, if no filter is provided with the query), as well as the number of pending orders, corresponding super-aggregates can be defined like this:

```javascript
{
    ...
    'Order': {
        table: 'orders',
        properties: {
            'id': {
                valueType: 'number',
                role: 'id'
            },
            ...
            'status': {
                valueType: 'string'
            },
            ...
            'items': {
                valueType: 'object[]',
                table: 'order_items',
                parentIdColumn: 'order_id',
                properties: {
                    'id': {
                        valueType: 'number',
                        role: 'id'
                    },
                    'productRef': {
                        valueType: 'ref(Product)',
                        column: 'product_id',
                        modifiable: false
                    },
                    'quantity': {
                        valueType: 'number'
                    }
                }
            }
        },
        superProperties: {
            'countPending': {
                valueType: 'number',
                aggregate: {
                    collection: 'records',
                    valueExpr: 'id => count',
                    filter: [
                        [ 'status => is', 'PENDING' ]
                    ]
                }
            },
            'totalAmount': {
                valueType: 'number',
                aggregate: {
                    collection: 'records.items',
                    valueExpr: 'productRef.price * quantity => sum'
                }
            }
        }
    },
    ...
}
```

Note that the records collection is referred in the super-aggregate expressions as `records`.

When a query with super-aggregates is executed, the super-aggregates are not a subject to the range specification, if any. They always reflect all records matching the filter regardless of the requested range.

## Fetch DBO

The fetch DBO is used to search records of a given record type and fetch the requested record data. The DBO is created using DBO factory's `buildFetch()` method, which takes the record type name and the query specification:

```javascript
const fetchDBO = dboFactory.buildFetch('Order', {
    // the query specification goes here
});
```

Once built, the DBO can be used and reused multiple times, which may be helpful as, depending on the complexity of the query specification, the DBO construction may be a relatively costly operation.

To execute the DBO, its `execute()` method is called, which takes up to three arguments:

* `txOrCon` - Database connection (or transaction object, described later in this manual).

* `actor` - Optional actor performing the operation. If not provided, the operation is anonymous.

* `filterParams` - If filter specification used to construct the DBO utilizes query parameters, this object provides the values for the parameters. Using query parameters in filter specifications helps making DBOs reusable. The keys in the provided object are parameter names and the values are the parameter values. Note, that when parameters are substituted in the SQL statement, no type transformation is performed by the framework. JavaScript strings are inserted as SQL strings, numbers as numbers, etc. Datetimes must be supplied as string in ISO format. References must be supplied as the referred record ids (strings or numbers depending on the referred record id value type).

The `execute()` method returns a `Promise`, which is fulfilled with the query result object. The query result object includes:

* `recordTypeName`, which is the requested record type name.

* `records` array of matched records, or empty array if none matched.

* `referredRecords`, which is included if any referred records were requested to be fetched along with the main record set. The keys in the object are record references and the values are objects representing the referred records.

* any requested super-properties.

The promise is rejected if an error occurs.

The query specification is an object that includes up to four sections:

* Specification of what record properties, referred records and super-properties to include in the result. Specified by the `props` array.

* The filter specification that asks the DBO to include only those records that match the criteria. Specified by the `filter` array.

* The order specification, which tells the DBO how to order the records in the result's `records` array. Specified by the `order` array.

* And the range specification that tells the DBO to return only the specified subrange of all the matched records. Specified by the `range` two-element array.

If no query specification is provided to the `buildFetch()` method, all records of the requested record type are included in the result with all the properties that are fetched by default (normally that includes all stored properties) and in no particular order. No referred records are fetched and no super-aggregates either.

### Selected Properties Specification

By default, the fetch DBO will fetch all stored properties of the matched records. Those include all properties that are not views, not calculated, not aggregates and not dependent record references. This default behavior can be overridden by using `fetchByDefault` attribute on the property definition, which can be either `true` or `false`, but generally is not recommended. Instead, to request only specific properties, the query specification can include a `props` property that is an array of property pattern strings. Each pattern can be:

* A star "*" to include all properties fetched by default. In the essence, not providing a query with a properties specification is equivalent to providing it with `props` equal `[ '*' ]`.

* Specific property path in dot notation. All intermediate properties are automatically included as well. If the end property is a nested object, all of its properties that are fetched by default are included. If any of the intermediate properties is a reference, the referred record is fetched and added to the `referredRecords` collection in the result object (only the properties explicitely selected are included, unless a wildcard pattern is used described next).

* A wildcard pattern, which is a property path in dot notation ending with ".*". This instructs the DBO to fetch the property and all of its children that would be fetched by default. In particular, if the property path is for a reference property, this allows fetching referred records and returning them in the `referredRecords` collection in the result object with all of their properties fetched by default.

* Specific property path in dot notation prefixed with a dash "-" to exclude a property that would otherwise be included (as a consequence of a wildcard pattern, for example).

* A super-property name prefixed with a dot "." to include the super-property.

See [Fetching Records](#fetching-records) usage section for an example.

### Filter Specification

By default, all records of the requested record type are matched. To restrict the matched records set, a `filter` property can be included in the query specification. The filter property is an array of _filter terms_ (also known as _filter specification elements_). Each term can be either a specification of a test to perform on the record, or a logical junction of nested filters.

For example, to select all _Order_ records for orders that were placed before a certain date and that have either status "PENDING" or "PROCESSING", the following filter specification can be used:

```javascript
const fetchDBO = dboFactory.buildFetch('Order', {
    ...
    filter: [
        [ 'placedOn => lt', '2017-01-01T00:00:00.000Z' ],
        [ ':or', [
            [ 'status => is', 'PENDING' ],
            [ 'status => is', 'PROCESSING' ]
        ]]
    ],
    ...
});
```

Alternatively, this could be written as:

```javascript
const fetchDBO = dboFactory.buildFetch('Order', {
    ...
    filter: [
        [ 'placedOn => lt', '2017-01-01T00:00:00.000Z' ],
        [ 'status => oneof', 'PENDING', 'PROCESSING' ]
    ],
    ...
});
```

But then we wouldn't see the use of the `:or` logical junction.

#### Logical Junctions

A logical junction is a filter term that is specified by a two-element array. The first element of the array is a string that describes how the nested filter terms are logically combined. The second element of the array is an array itself that consists of the filter terms that are combined in the junction.

The following junction types are supported:

* `:or`, `:any`, `:!none` - Disjunction. The nested filter terms are combined using logical _OR_.

* `:!or`, `:!any`, `:none` - Inverted disjunction. The nested filter terms are combined using logical _OR_ and then negated using logical _NOT_.

* `:and`, `:all` - Conjunction. The nested filter terms are combined using logical _AND_.

* `:!and`, `:!all` - Inverted conjunction. The nested filter terms are combined using logical _AND_ and then negated using logical _NOT_.

The top `filter` property in a query specification does not need to be an explicit logical junction if it is a conjunction (an `:and`).

#### Tests

Each test is a filter term that is defined as an array with one or more elements. The first element is called the _predicate_ and is always a string that expresses what is tested and what is the test. If the test requires supplementary values to test against&mdash;the test parameters&mdash;the values follow the predicate as the rest of the filter term array elements.

The predicate is defined as:

```
Expr => Test
```

Where `Expr` is a value expression as in the [Calculated Properties](#calculated-properties). This is the value that is tested. The `Test` is one of:

* `is`, `eq` - Test if the value is equal to the single test parameter.

* `not`, `ne`, `!eq` - Test if the value is not equal to the single test parameter.

* `min`, `ge`, `!lt` - Test if the value is greater or equal to the single test parameter.

* `max`, `le`, `!gt` - Test if the value is less or equal to the single test parameter.

* `gt` - Test if the value is strictly greater than the single test parameter.

* `lt` - Test if the value is strictly less than the single test parameter.

* `in`, `oneof`, `alt` - Test if the value is equal to any of the test parameters. The test take any number of parameters, or it takes an array of values as a parameter.

* `!in`, `!oneof` - Test if the value is not equal to any of the test parameters.

* `between` - Test if the value is greater or equal to the first test parameter and less or equal to the second test parameter.

* `!between` - Test if the value is less than the first test parameter or greater than the second test parameter.

* `contains` - Test if the value contains the substring provided as the single test parameter. The substring is treated as case-sensitive.

* `containsi`, `substring` - Same as `contains`, but case-insensitive.

* `!contains` - Inversion of `contains`.

* `!containsi`, `!substring` - Inversion of `containsi`.

* `starts` - Test if the value starts with the string provided as the single test parameter. The string is treated as case-sensitive.

* `startsi`, `prefix` - Same as `starts`, but case-insensitive.

* `!starts` - Inversion of `starts`.

* `!startsi`, `!prefix` - Inversion of `startsi`.

* `matches` - Test if the value matches the regular expression provided as the single test parameter. The match is performed as case-sensitive.

* `matchesi`, `pattern`, `re` - Same as `matches`, but case-insensitive.

* `!matches` - Inversion of `matches`.

* `!matchesi`, `!pattern`, `!re` - Inversion of `matchesi`.

* `empty` - Test if there is no value (the value is absent, `undefined` or `null`). This test does not take any parameters.

* `!empty`, `present` - Test if the value is not empty.

If the test is not provided at all (there is no `=>` followed by the test in the predicate) and there are no test parameters, then `!empty` is assumed. If there are parameters, then `eq` is assumed.

The parameters to the tests can be provided in several different forms. The simplest way is to provide the value as is:

```javascript
filter = [
    [ 'name', 'John' ],
    [ 'status => oneof', 'PENDING', 'PROCESSING' ],
    [ 'quantity => between', 10, 20 ],
    [ 'isAccepted', true ],
    [ 'placedOn => min', '2017-02-01T10:00:00.000Z' ],
    [ 'accountRef', 10 ],
	[ 'zip => present' ],
    [ 'length(concat(lastName, ", ", firstName)) => max', 30 ]
]
```

The type of the test parameter value must match the predicate's expression. The framework does not perform the conversions assuming that the application knows what value to provide for the specific test. Note, that the values for `datetime` fields are provided as ISO strings and values for reference properties are provided as the referred record ids, which can be either strings or numbers depending on the record id property value type.

Specifying test parameters to filters as values has the effect of "baking in" the parameters into the DBOs, which limits their reusability. It is possible to create a parameterized DBO if filter test parameters are specified not as values, but as _filter parameters_. To create a named filter parameter placeholder, the module's `param()` function is used:

```javascript
const dbos = require('x2node-dbos');

...

filter = [
    [ 'name', dbos.param('name') ],
    [ 'status => oneof', dbos.param('statuses') ],
    [ 'accountRef', dbos.param('accountId') ]
];
```

The values for the filter parameters are provided when the DBO is executed:

```javascript
resultPromise = fetchDBO.execute(connection, actor, {
    name: 'John',
	statuses: [ 'PENDING', 'PROCESSING' ],
	accountId: 10
});
```

And finally, sometimes it is necessary to test the predicate not against a value known to the application, but against another expression. This is made possible with the use of the module's `expr()` function:

```javascript
filter = [
    [ 'accountRef => is', dbos.expr('ownerAccountRef') ]
    [ 'length(firstName) => gt', dbos.expr('length(lastName)') ]
];
```

Using property paths in dot notation allows constructing complex tests that involve values even from different related records.

#### Collection Tests

TODO

### Order Specification

TODO

### Range Specification

TODO

## Insert DBO

TODO

## Update DBO

TODO

## Delete DBO

TODO

## Transactions

TODO

## Data Sources

TODO

## Database Drivers

TODO
