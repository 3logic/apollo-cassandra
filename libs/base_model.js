var util = require('util'),
    build_error = require('./apollo_error.js'),
    cql = require('cassandra-driver'),
    schemer = require('./apollo_schemer'),
    async = require('async'),
    lodash = require('lodash');

/*
Valid consistencies:
any
one
two
three
quorum
all
localQuorum
eachQuorum
localOne */
var CONSISTENCY_FIND   = 'quorum',
    CONSISTENCY_SAVE   = 'quorum',
    CONSISTENCY_DEFINE = 'all',
    CONSISTENCY_DELETE = 'quorum',
    CONSISTENCY_DEFAULT = 'quorum';

/**
 * Consistency levels
 * @typedef {Object} BaseModel~cql_consistencies
 * @readonly
 * @enum {number}
 */
var cql_consistencies = cql.types.consistencies;

var TYPE_MAP = require('./cassandra_types');
var check_db_tablename = function (obj){
    return ( typeof obj == 'string' && /^[a-z]+[a-z0-9_]*/.test(obj) );
};

var noop = function(){};

/**
 * Build a row (a model instance) for this model
 * @param {object} instance_values Key/value object containing values of the row
 * @class
 * @classdesc Base class for generated models
 */
var BaseModel = function(instance_values){
    instance_values = instance_values || {};
    var _field_values = {};
    var fields = this.constructor._properties.schema.fields;
    var self = this;
    var default_setter = function(prop_name, new_value){
            this[prop_name] = new_value;
        },
        default_getter = function(prop_name){
            return this[prop_name];
        },
        validation_wrapper = function(setter, validation_func, prop_name, fieldtype){
            return function(value){
                if(!self._skip_validation){
                    var validation_result = validation_func(value);
                    if( validation_result !== true )
                        throw build_error('model.set.invalidvalue', validation_result(value, prop_name, fieldtype) );
                }
                setter(value);
            };
        },
        generic_validator_message_func = function(value, prop_name, fieldtype){return util.format('Invalid Value: "%s" for Field: %s (Type: %s)', value, prop_name, fieldtype); };
    this._validators = {};
    this._skip_validation = false;
    var validators;

    for(var fields_keys = Object.keys(fields), i = 0, len = fields_keys.length; i < len; i++){
        var property_name = fields_keys[i],
            field = fields[fields_keys[i]],
            fieldtype = schemer.get_field_type(this.constructor._properties.schema,fields_keys[i]),
            fieldvalue = instance_values[fields_keys[i]];


        var type_fieldvalidator = TYPE_MAP.generic_type_validator(TYPE_MAP[fieldtype].validator);
        validators = [type_fieldvalidator];
        if( typeof field.rule != 'undefined' ){
            if( typeof field.rule === 'function'){
                field.rule = {
                    validator : field.rule,
                    message   : generic_validator_message_func
                };
            }else{
                if( typeof field.rule != 'object' || typeof field.rule.validator == 'undefined' ){
                    throw 'Invalid validator';
                }
                if(!field.rule.message){
                    field.rule.message = generic_validator_message_func
                }else if( typeof field.rule.message == 'string' ){
                    field.rule.message = function(message, value, prop_name, fieldtype){return util.format(message, value, prop_name, fieldtype); }.bind(null, field.rule.message);
                }else if( typeof field.rule.message != 'function' ){
                    throw 'Invalid validator message';
                }
            }
            validators.push(field['rule']);
        }
        this._validators[property_name] = validators;

        var validation_func = this.constructor._validate.bind(this.constructor, validators);

        var setter = validation_wrapper( default_setter.bind(_field_values, property_name ), validation_func, property_name, fieldtype),
            getter = default_getter.bind(_field_values, property_name);

        if(field['virtual'] && typeof field['virtual']['set'] === 'function'){
            setter = validation_wrapper(field['virtual']['set'].bind(_field_values), validation_func, property_name, fieldtype);
            //field['virtual']['set'].bind(_field_values);
        }

        if(field['virtual'] && typeof field['virtual']['get'] === 'function'){
            getter = field['virtual']['get'].bind(_field_values);
        }


        var descriptor = {
            enumerable: true,
            set : setter,
            get : getter
        };

        Object.defineProperty(this, property_name, descriptor);
        this[property_name] = instance_values[property_name];
    }

};

/* Static Private ---------------------------------------- */

/**
 * Properties of the model
 * @protected
 * @abstract
 * @type {Object}
 */
BaseModel._properties = {
    name : null,
    schema : null
};

/**
 * Set properties for the model. Creation of Model constructor use this method to set internal properties
 * @param {object} properties Properties object
 * @protected
 */
BaseModel._set_properties = function(properties){
    var schema = properties.schema,
        cql = properties.cql,
        table_name = schema.table_name || properties.name;

    if(!check_db_tablename(table_name)){
        throw(build_error('model.tablecreation.invalidname',table_name));
    }

    var qualified_table_name = properties.keyspace + '.' + table_name;

    this._properties = properties;
    this._properties.table_name = table_name;
    this._properties.qualified_table_name = qualified_table_name;
};

/**
 * Calls a list of validator on a value
 * @param  {array} validators - Array of validation functions
 * @param  {*} value      - The value to validate
 * @return {(boolean|function)}            True or a function which generate validation message
 * @protected
 */
BaseModel._validate = function(validators, value){
    if( typeof value == 'undefined' || value == null || (typeof value == 'object' && value['$db_function']))
        return true;
    for(var v in validators){
        if(!validators[v].validator(value)){
            return validators[v].message;
        }
    }
    return true;
}

BaseModel._ensure_connected = function(callback){
    if(!this._properties.cql){
        this._properties.connect(callback);
    }else{
        callback();
    }
};

/**
 * Execute a query on a defined connection which always remain the same
 * @param  {string}                         query       Query to execute
 * @param  {object}                         options     Options for the query
 * @param  {BaseModel~cql_consistencies}    consistency Consistency type
 * @param  {BaseModel~GenericCallback}      callback    callback of the execution
 * @protected
 * @static
 */
BaseModel._execute_definition_query = function(query, params, consistency, callback){
    this._ensure_connected(function(err){
        if(err){
            return callback(err);
        }
        var properties = this._properties,
            conn = properties.define_connection;
        conn.execute(query, params, {'prepare': false, 'consistency': consistency, 'fetchSize': 0}, callback);
    }.bind(this));
};

/**
 * Execute queries in batch on A connection
 * @param  {object[]}   queries     query, params object
 * @param  {BaseModel~cql_consistencies}   consistency Consistency type
 * @param  {BaseModel~GenericCallback}      callback    callback of the execution
 * @protected
 * @static
 */
BaseModel._execute_batch = function(queries, consistency, callback){
    this._ensure_connected(function(err){
        if(err) return callback(err);
        consistency = (typeof consistency == 'string' ? cql_consistencies[consistency] : consistency);
        this._properties.cql.batch(queries, {'consistency': consistency} , callback);
    }.bind(this));
};


/**
 * Create table on cassandra for this model
 * @param  {BaseModel~GenericCallback} callback Called on creation termination
 * @protected
 * @static
 */
BaseModel._create_table = function(callback){
    var properties = this._properties,
        table_name = properties.table_name,
        model_schema = properties.schema,
        mismatch_behaviour = properties.mismatch_behaviour,
        cql = properties.cql;

    var consistency = cql_consistencies[CONSISTENCY_DEFINE];

    //check for existence of table on DB and if it matches this model's schema
    this._get_db_table_schema(function(err,db_schema){

        if(err) return callback(err);

        var after_dbcreate = function(err, result){
            if (err) return callback(build_error('model.tablecreation.dbcreate', err));
            //index creation
            if(model_schema.indexes instanceof Array){
                async.eachSeries(model_schema.indexes, function(idx,next){
                    this._execute_definition_query(this._create_index_query(table_name,idx), [], consistency, function(err, result){
                        if (err) next(build_error('model.tablecreation.dbindex', err));
                        else
                            next(null,result);
                    });
                }.bind(this),callback);
            }
            else
                callback();

        }.bind(this);


        if (db_schema){// check if schemas match
            var normalized_model_schema = schemer.normalize_model_schema(model_schema),
                normalized_db_schema = schemer.normalize_model_schema(db_schema);

            if (!lodash.isEqual(normalized_model_schema, normalized_db_schema)){
                if(mismatch_behaviour === 'drop'){
                    this.drop_table(function(err,result){
                        if (err) return callback(build_error('model.tablecreation.dbcreate', err));
                        var  create_query = this._create_table_query(table_name,model_schema);
                        this._execute_definition_query(create_query, [], consistency, after_dbcreate);

                    }.bind(this));
                } else{
                    return callback(build_error('model.tablecreation.schemamismatch', table_name));
                }
            }
            else callback();
        }
        else{  // if not existing, it's created anew
            var  create_query = this._create_table_query(table_name,model_schema);
            this._execute_definition_query(create_query, [], consistency, after_dbcreate);
        }
    }.bind(this));
};

/**
 * Generate a query to create this model table
 * @param  {string} table_name Model table name
 * @param  {object} schema     Schema of the model
 * @return {string}            The creation query
 * @protected
 */
BaseModel._create_table_query = function(table_name,schema){
    var rows = [],
        field_type;
    for(var k in schema.fields){
        if(schema.fields[k].virtual){
            continue;
        }
        field_type = schemer.get_field_type(schema, k);
        rows.push(
            util.format(
                '"%s" %s %s',
                k,
                field_type,
                schema.fields[k]["static"] ? "static" : " " 
            )
        );
    }

    var partition_key = schema.key[0],
        clustering_key = schema.key.slice(1,schema.key.length);

    partition_key  = partition_key instanceof Array ? partition_key.map(function(v){return util.format('"%s"',v); }).join(",") : util.format('"%s"',partition_key);
    clustering_key = clustering_key.length ? ','+clustering_key.map(function(v){return util.format('"%s"',v); }).join(",") : '';

    query = util.format(
        'CREATE TABLE IF NOT EXISTS  "%s" (%s , PRIMARY KEY((%s)%s));',
        table_name,
        rows.join(" , "),
        partition_key,
        clustering_key
    );

    return query;
};


/**
 * Create the qery to generate table index
 * @param  {string} table_name Name of the table
 * @param  {string} index_name Name of the field to index
 * @return {string}            The index creation query
 * @protected
 */
BaseModel._create_index_query = function(table_name, index_name){
    var query = util.format(
        "CREATE INDEX IF NOT EXISTS ON %s (%s);",
        table_name,
        index_name
    );
    return query;
};


/**
 * Get the schema from an existing table on Cassandra
 * @param  {BaseModel~GetDbSchema} callback - The callback populated with the retrieved schema
 * @protected
 */
BaseModel._get_db_table_schema = function (callback){
    var table_name = this._properties.table_name,
        keyspace = this._properties.keyspace;

    var query = "SELECT * FROM system.schema_columns WHERE columnfamily_name = ? AND keyspace_name = ? ALLOW FILTERING;";

    this.execute_query(query,[table_name,keyspace], null, function(err, result) {
        if (err) return callback(build_error('model.tablecreation.dbschemaquery', err));

        if(!result.rows || result.rows.length === 0)
            return callback(null, null);

        var db_schema = {fields:{}};
        for(var r in result.rows){
            var row = result.rows[r];
            db_schema.fields[row.column_name] = TYPE_MAP.find_type_by_dbvalidator(row.validator);
            if(row.type == 'partition_key'){
                if(!db_schema.key)
                    db_schema.key = [[]];
                db_schema.key[0][row.component_index||0] = row.column_name;
            }
            else if(row.type == 'clustering_key'){
                if(!db_schema.key)
                    db_schema.key = [[]];
                db_schema.key[row.component_index+1] = row.column_name;
            }
            else if(row.type == 'static'){
                if(!db_schema['static'])
                    db_schema['static'] = [];
                db_schema['static'].push(row.column_name);
            }
            if(row.index_name){
                if(!db_schema.indexes)
                    db_schema.indexes = [];
                db_schema.indexes.push(row.column_name);
            }
        }

        callback(null,db_schema);
    }.bind(this));

};


/**
 * Execute a query which involves the model table
 * @param  {string}   query     The query to execute
 * @param  {BaseModel~cql_consistencies}   consistency Consistency type
 * @param  {BaseModel~QueryExecution} callback  Callback with err and result
 * @protected
 */
//BaseModel._execute_table_query = BaseModel._execute_definition_query;
BaseModel._execute_table_query = function(query, params, consistency, callback){

    var do_execute_query = function(doquery,docallback){
        this.execute_query(doquery, params, consistency, docallback);
    }.bind(this,query);

    if(this.is_table_ready()){
        do_execute_query(callback);
    }
    else{
        this.init(function(err){
            if(err){
                return callback(err);
            }
            do_execute_query(callback);
        });
    }

};


/**
 * Given a field name and a value, format the query portion regarding that value
 * @param  {string} fieldname  Name of the field
 * @param  {string} fieldvalue Value of the field
 * @return {string}            String to be used in query
 * @protected
 * @throws Error if invalid field value given its type
 *
 */
BaseModel._get_db_value_expression = function(fieldname, fieldvalue){
    /* jshint sub: true */

    var fieldtype = schemer.get_field_type(this._properties.schema, fieldname);

    if(fieldvalue === null){
        return 'NULL';
    }

    if(typeof fieldvalue == 'object'){
        if(fieldvalue['$db_function'])
            return fieldvalue['$db_function'];
    }

    if(fieldvalue instanceof Array){
        var val = fieldvalue.map(function(v){
                return this._get_db_value_expression(fieldname, v);
            }.bind(this)).join(', ');
        return util.format('(%s)',val);
    }

    switch(fieldtype){
        case 'text':
        case 'varchar':
        case 'ascii':
            return util.format("'%s'",fieldvalue.replace(/'/g, "''"));
        case 'inet':
            return util.format("'%s'",fieldvalue);
        case 'timestamp':
            if( !(fieldvalue instanceof Date) )
                fieldvalue = new Date(fieldvalue);
            if( isNaN( fieldvalue.getTime() ) )
                throw(build_error('model.save.invalidvalue',fieldvalue,fieldname,fieldtype));

            return ("\'" + fieldvalue.toISOString().replace(/\..+/, '') + "\'");
        case 'blob':
            return util.format("textAsBlob('%s')",fieldvalue.toString());
        case 'uuid':
        case 'timeuuid':
            return util.format("%s",fieldvalue.toString());
        default:
            return fieldvalue;
    }
};

/**
 * Given a complete query object, generate the where clause part
 * @param  {object} query_ob Object representing the query
 * @return {string}          Where clause
 * @protected
 */
BaseModel._create_where_clause = function(query_ob){
    var query_relations = [];
    for(var k in query_ob){
        if( k.indexOf('$') === 0 ){
            continue;
        }
        var where_object = query_ob[k];
        //Array of operators
        if( !(where_object instanceof Array))
            where_object = [where_object];
        for (var fk in where_object){
            var field_relation = where_object[fk];
            if(typeof field_relation == 'number' || typeof field_relation == 'string' || typeof field_relation == 'boolean' )
                field_relation = {'$eq': field_relation};
            else if(typeof field_relation != 'object')
                throw(build_error('model.find.invalidrelob'));

            var rel_keys = Object.keys(field_relation);
            if(rel_keys.length > 1)
                throw(build_error('model.find.multiop'));

            var cql_ops = {'$eq':'=', '$gt':'>', '$lt':'<', '$gte':'>=', '$lte':'<=', '$in':'IN'};

            var first_key = rel_keys[0],
                first_value = field_relation[first_key];
            if(first_key.toLowerCase() in cql_ops){
                first_key = first_key.toLowerCase();
                var op = cql_ops[first_key];

                if(first_key == '$in' && !(first_value instanceof Array))
                    throw(build_error('model.find.invalidinset'));
                query_relations.push( util.format(
                    '%s %s %s',
                    k,op,this._get_db_value_expression(k,first_value)
                ));
            }
            else {
                throw(build_error('model.find.invalidop',first_key));
            }
        }
    }
    return query_relations.length > 0 ? util.format('WHERE %s',query_relations.join(' AND ')) : '';
};

/**
 * Given a complete query object, generate the SELECT query
 * @param  {object} query_ob Object representing the query
 * @param  {object} options  Options for the query. Unused right now
 * @return {string}          Select statement
 * @protected
 */
BaseModel._create_find_query = function(query_ob, options){
    var query_relations = [],
        order_keys = [],
        limit = null;

    for(var k in query_ob){
        var query_item = query_ob[k];
        if(k.toLowerCase() === '$orderby'){
            if(!(query_item instanceof Object)){
                throw(build_error('model.find.invalidorder'));
            }
            var order_item_keys = Object.keys(query_item);
            if(order_item_keys.length > 1)
                throw(build_error('model.find.multiorder'));

            var cql_orderdir = {'$asc':'ASC', '$desc':'DESC'};
            if(order_item_keys[0].toLowerCase() in cql_orderdir){

                var order_fields = query_item[order_item_keys[0]];

                if(!(order_fields instanceof Array))
                    order_fields = [order_fields];

                for(var i in order_fields){
                    order_keys.push(util.format(
                        '%s %s',
                        order_fields[i], cql_orderdir[order_item_keys[0]]
                    ));
                }
            }else{
                throw(build_error('model.find.invalidordertype', order_item[order_item_keys[0]]));
            }
        }
        else if(k.toLowerCase() === '$limit'){
            if(typeof query_item !== 'number')
                throw(build_error('model.find.limittype'));
            limit = query_item;
        }
    }
    var where = this._create_where_clause(query_ob);
    var query = util.format(
        'SELECT * FROM "%s" %s %s %s ALLOW FILTERING;',
        this._properties.table_name,
        where,
        order_keys.length ? 'ORDER BY '+ order_keys.join(', '):' ',
        limit ? 'LIMIT '+limit : ' '
    );
    return query;
};




/* Static Public ---------------------------------------- */

/**
 * Restituisce il nome della tabella usato dal modello
 * @return {string} Nome della tabella
 */
BaseModel.get_table_name = function(){
    return this._properties.table_name;
};

/**
 * Return true if data related to model is initialized on cassandra
 * @return {Boolean} The ready state
 * @public
 */
BaseModel.is_table_ready = function(){
    return this._ready === true;
};

/**
 * Initialize data related to this model
 * @param  {object}   options  Options
 * @param  {BaseModel~QueryExecution} callback Called on init end
 */
BaseModel.init = function(options, callback){
    if(!callback){
        callback = options;
        options = undefined;
    }

    var after_create = function(err, result){
        if(!err)
            this._ready = true;
        callback(err,result);
    }.bind(this);

    if(options && options.drop === true){
        this.drop_table(function(err){
            if(err) {return callback(build_error('model.tablecreation.dbdrop',err));}
            this._create_table(after_create);
        });
    }
    else {
        this._create_table(after_create);
    }
};

/**
 * Execute a generic query
 * @param  {string}                         query - Query to execute
 * @param  {BaseModel~cql_consistencies}    consistency - Consistency type
 * @param  {BaseModel~QueryExecution}       callback - Called on execution end
 */
BaseModel.execute_query = function(query, params, consistency, callback){
    this._ensure_connected(function(err){
        if(err) return callback(err);
        consistency = (typeof consistency == 'string' ? cql_consistencies[consistency] : consistency);
        this._properties.cql.execute(query, params, {'prepare': false, 'consistency': consistency, 'fetchSize': 0}, function(err, result){
            if(err && err.code == 8704){
                this._execute_definition_query(query, params, consistency, callback);
            }else{
                callback(err, result);
            }
        }.bind(this));
    }.bind(this));
};

/**
 * Execute a generic query
 * @param  {string}                         query - Query to execute
 * @param  {BaseModel~cql_consistencies}    consistency - Consistency type
 * @param  {BaseModel~QueryExecution}       callback - Called on execution end
 */
BaseModel.execute_prepared_query = function(query, consistency, callback){
    this._properties.cql.execute(query, cql_consistencies[consistency], callback);
};


/**
 * Execute a search on Cassandra for row of this Model
 * @param  {object}                   query_ob - The query objcet
 * @param  {BaseModel~find_options}   [options] - Option for this find query
 * @param  {BaseModel~QueryExecution} callback - Data retrieved
 */
BaseModel.find = function(query_ob, options, callback){
    if(arguments.length == 2){
        callback = options;
        options = {};
    }
    if(!callback)
        throw 'Callback needed!';

    var defaults = {
        raw : false,
        consistency : CONSISTENCY_FIND
    };

    options = lodash.defaults(options, defaults);

    var query;
    try{
        query = this._create_find_query(query_ob, options);
    }
    catch(e){
        return callback(e);
    }
    this._execute_table_query(query, null, options.consistency, function(err,results){
        if(err) return callback(build_error('model.find.dberror',err));
        if(!options.raw){
            var ModelConstructor = this._properties.get_constructor();
            results = results.rows.map(function(res){
                delete(res.columns);
                return new ModelConstructor(res);
            });
            callback(null,results);
        }else{
           results = results.rows.map(function(res){
                delete(res.columns);
                return res;
            });
            callback(null,results);
        }
    }.bind(this));

};

/**
 * Delete entry on database
 * @param  {object}                     query_ob - The query object for deletion
 * @param  {BaseModel~delete_options}   [options] - Option for this delete query
 * @param  {BaseModel~GenericCallback}  callback - Data retrieved
 */
BaseModel.delete = function(query_ob, options, callback){
    if(arguments.length == 2){
        callback = options;
        options = {};
    }
    if(!callback)
        throw 'Callback needed!';

    var defaults = {
        consistency : CONSISTENCY_DELETE
    };

    options = lodash.defaults(options, defaults);

    var query = 'DELETE FROM "%s" %s;',
        where = '';
    try{
        where = this._create_where_clause(query_ob, options);
    }
    catch(e){
        return callback(e);
    }
    query = util.format(query, this._properties.table_name, where);
    this._execute_table_query(query, null, options.consistency, function(err,results){
        if(err) return callback(build_error('model.delete.dberror',err));
        callback(null, results);
    });

};


/**
 * Drop table related to this model
 * @param  {BaseModel~GenericCallback} callback in case of error returns it
 */
BaseModel.drop_table = function(callback){
    var properties = this._properties,
        table_name = properties.table_name,
        cql = properties.cql;

    var query = util.format('DROP TABLE IF EXISTS "%s";', table_name);
    this._execute_definition_query(query,[],cql_consistencies[CONSISTENCY_DEFINE],callback);
};


/* Instance Private --------------------------------------------- */

/**
 * Set of validators for fields
 * @private
 * @type {Object}
 */
//BaseModel.prototype._validators = {};


BaseModel.prototype._get_default_value = function(fieldname){
    var properties = this.constructor._properties,
        schema = properties.schema,
        fieldtype = schemer.get_field_type(schema, fieldname);

    if (typeof schema.fields[fieldname] == 'object' && schema.fields[fieldname].default !== undefined){
        if(typeof schema.fields[fieldname].default == 'function'){
            return schema.fields[fieldname].default.call(this);
        }
        else
            return schema.fields[fieldname].default;
    }
    else
        return undefined;
};


/**
 * Update model instance with values from DB
 * @param  {BaseModel~GenericCallback} callback in case of error returns it
 */
BaseModel.prototype._update_self = function(callback){
    var properties = this.constructor._properties,
        schema = properties.schema,
        partition_keys = typeof schema.key[0] === 'string' ? [schema.key[0]] : schema.key[0],
        clustering_keys = schema.key.slice(1),
        keys = partition_keys.concat(clustering_keys),
        query_obj = {};


    for(var k in keys){
        query_obj[keys[k]] = this[keys[k]];
    }

    this.constructor.find(query_obj, {'raw':true}, function(err, result){
        if(err)
            return callback(err);
        if(result.length!==1) {
            callback('Error self-updating model instance: not a single record in DB',result);
        }
        var f;
        this._skip_validation = true;
        for (f in properties.schema.fields){
            this[f] = result[0][f];
        }
        this._skip_validation = false;
        callback();
    }.bind(this));
};


/* Instance Public --------------------------------------------- */


/**
 * Validate a property given its name
 * @param  {string} property_name - Name of the property to validate
 * @param  {*} [value=this[property_name]] - Value to validate. If not provided the current instance value
 * @return {boolean}              False if validation fails
 */
BaseModel.prototype.validate = function( property_name, value ){
    value = value || this[property_name];
    this._validators = this._validators || {};
    return this.constructor._validate(this._validators[property_name] || [], value);
}

/**
 * Save this instance of the model
 * @param  {BaseModel~save_options}     [options] - options for the query
 * @param  {BaseModel~QueryExecution}   callback - Result of the save or an error eventually
 * @instance
 */
BaseModel.prototype.save = function(options, callback){
    if(arguments.length == 1){
        callback = options;
        options = {};
    }

    var identifiers = [], values = [],
        properties = this.constructor._properties,
        schema = properties.schema,
        defaults = {
            consistency : CONSISTENCY_SAVE
        };

    var must_update_self = false;

    options = lodash.defaults(options, defaults);

    for(var f in schema.fields){
        if(schema.fields[f]['virtual'])
            continue;

        // check field value
        var fieldtype = schemer.get_field_type(schema,f),
            fieldvalue = this[f];

        if (fieldvalue === undefined || schema.fields[f]['auto']){
            fieldvalue = this._get_default_value(f);

            if(fieldvalue === undefined){
                if(schema.key.indexOf(f) >= 0 || schema.key[0].indexOf(f) >= 0)
                    return callback(build_error('model.save.unsetkey',f));
                else
                    continue;
            } else {
                if(fieldvalue['$db_function'])
                    must_update_self = true;
                if(!schema.fields[f].rule || !schema.fields[f].rule.ignore_default){ //did retrieve a default value, ignore default is not set
                    if( this.validate( f, fieldvalue ) !== true ){
                        return callback(build_error('model.save.invaliddefaultvalue',fieldvalue,f,fieldtype));
                    }
                }
            }
        }

        if(fieldvalue === null){
            if(schema.key.indexOf(f) >= 0 || schema.key[0].indexOf(f) >= 0)
                return callback(build_error('model.save.unsetkey',f));
        }

        identifiers.push(f);

        try{
            values.push(this.constructor._get_db_value_expression(f,fieldvalue));
        }
        catch(e){
            return callback(build_error('model.save.invalidvalue',fieldvalue,f,fieldtype));
        }

        if(!fieldvalue || !fieldvalue['$db_function']){
            this._skip_validation = true;
            if(this[f]!==fieldvalue)
                this[f] = fieldvalue;
            this._skip_validation = false;
        }

    }

    var query = util.format(
        "INSERT INTO %s ( %s ) VALUES ( %s )",
        properties.qualified_table_name,
        identifiers.join(" , "),
        values.join(" , ")
    );
    this.constructor._execute_table_query(query, null,options.consistency, function(err, result){
        if(err) return callback(err);
        if(must_update_self){
            this._update_self(function(err){
                if(err)
                    console.warn(err);
                callback(null, result);
            });
        }
        else
            callback(null, result);
    }.bind(this));
};

/**
 * Delete this entry on database
 * @param  {BaseModel~delete_options}   [options={}] - Option for this delete query
 * @param  {BaseModel~GenericCallback}  callback - Data retrieved
 */
BaseModel.prototype.delete = function(options, callback){
    if(arguments.length == 1){
        callback = options;
        options = {};
    }
    var schema = this.constructor._properties.schema;
    var delete_query = {};

    for(var i in schema.key){
        delete_query[schema.key[i]] = this[schema.key[i]];
    }
    this.constructor.delete(delete_query, options, callback);
};

module.exports = BaseModel;

/**
 * Generic callback with just error parameter.
 * @callback BaseModel~GenericCallback
 * @param {object} err
 */

/**
 * Generic callback with just error parameter.
 * @callback BaseModel~GetDbSchema
 * @param {object} err - Eventually the error
 * @param {object} schema - The schema retrieved
 */

/**
 * Generic callback with just error parameter.
 * @callback BaseModel~QueryExecution
 * @param {object} err - Eventually the error
 * @param {object} result - The data retrieved
 */

/**
* Options for find operation
* @typedef {Object} BaseModel~find_options
* @property {boolean} [raw=false] - Returns raw result instead of instances of your model
* @property {string} [consistency=CONSISTENCY_FIND] - Define consistency for this operation
*/

/**
* Options for delete operation
* @typedef {Object} BaseModel~delete_options
* @property {string} [consistency=CONSISTENCY_DELETE] - Define consistency for this operation
*/

/**
* Options for save operation
* @typedef {Object} BaseModel~save_options
* @property {string} [consistency=CONSISTENCY_SAVE] - Define consistency for this operation
*/