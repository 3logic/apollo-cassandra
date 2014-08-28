var util = require('util'),
    build_error = require('./apollo_error.js'),
    cql = require('node-cassandra-cql'),
    schemer = require('./apollo_schemer'),
    async = require('async'),
    lodash = require('lodash');

var CONSISTENCY_FIND   = 'one',
    CONSISTENCY_SAVE   = 'one',
    CONSISTENCY_DEFINE = 'one',
    CONSISTENCY_DELETE = 'one';

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
    var _fields = {};
    var fields = this.constructor._properties.schema.fields;

    var set_func = function(prop_name, new_value){
            _fields[prop_name] = new_value;
        },
        get_func = function(prop_name){
            return _fields[prop_name];
        };
    
    for(var fields_keys = Object.keys(fields), i = 0, len = fields_keys.length; i < len; i++){
        var property_name = fields_keys[i];
        var descriptor = {
            enumerable: true,
            set : set_func.bind(null, property_name),
            get: get_func.bind(null, property_name)
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

    var qualified_table_name = cql.options.keyspace+'.'+table_name;

    this._properties = properties;
    this._properties.table_name = table_name;
    this._properties.qualified_table_name = qualified_table_name;
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
BaseModel._execute_definition_query = function(query, options, consistency, callback){
    var properties = this._properties,
        conn = properties.define_connection;

    conn.open(function(){
         conn.execute(query, options, consistency, callback);
    });
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

    //controllo esistenza della tabella ed eventuale corrispondenza con lo schema
    this._get_db_table_schema(function(err,db_schema){
        //console.log('get schema', arguments);
        if(err) return callback(err);

        var after_dbcreate = function(err, result){
            //console.log('after create', arguments);
            if (err) return callback(build_error('model.tablecreation.dbcreate', err));   
            //creazione indici  
            if(model_schema.indexes instanceof Array)
                async.eachSeries(model_schema.indexes, function(idx,next){
                    //console.log(this._create_index_query(table_name,idx));
                    cql.execute(this._create_index_query(table_name,idx), [], consistency, function(err, result){
                        if (err) next(build_error('model.tablecreation.dbindex', err));
                        else
                            next(null,result);
                    });
                }.bind(this),callback);
            else
                callback();
        }.bind(this);      


        if (db_schema){// check if schemas match

            schemer.normalize_model_schema(model_schema);     
            schemer.normalize_model_schema(db_schema);     

            if (!lodash.isEqual(model_schema, db_schema)){
                //console.log('mismatch', model_schema, db_schema);
                if(mismatch_behaviour === 'drop'){
                    this._drop_table(function(err,result){
                        //console.log('after drop', arguments);
                        if (err) return callback(build_error('model.tablecreation.dbcreate', err));
                        //console.log(this._create_table_query(table_name,model_schema));
                        //cql.execute(this._create_table_query(table_name,model_schema), [], consistency, after_dbcreate);
                        this._execute_definition_query(this._create_table_query(table_name,model_schema), [], consistency, after_dbcreate);
                      
                    }.bind(this));
                } else
                    return callback(build_error('model.tablecreation.schemamismatch', table_name));
            }
            else callback();               
        }
        else{  // if not existing, it's created anew
            //console.log('create'); 

            //cql.execute(this._create_table_query(table_name,model_schema), [], consistency, after_dbcreate);
            this._execute_definition_query(this._create_table_query(table_name,model_schema), [], consistency, after_dbcreate);
        }
    }.bind(this));
};

/**
 * Drop a table
 * @param  {BaseModel~GenericCallback} callback - return eventually an error on dropping
 * @protected
 */
BaseModel._drop_table = function(callback){
    var properties = this._properties,
        table_name = properties.table_name,
        cql = properties.cql;

    var query = util.format('DROP TABLE IF EXISTS "%s";', table_name);
    this._execute_definition_query(query,[],cql_consistencies[CONSISTENCY_DEFINE],callback);
    //cql.execute(query,[],cql_consistencies[CONSISTENCY_SAVE],callback);
};

/**
 * Generate a query to create this model table 
 * @param  {string} table_name Model table name
 * @param  {object} schema     Schema of the model
 * @return {string}            The creation query
 * @protected
 */
BaseModel._create_table_query = function(table_name,schema){
    //creazione tabella
    var rows = [];
    for(var k in schema.fields)
        rows.push(k + " " + schema.fields[k]);

    var partition_key = schema.key[0],
        clustering_key = schema.key.slice(1,schema.key.length);

    partition_key  = partition_key instanceof Array ? partition_key.join(",") : partition_key;
    clustering_key = clustering_key.length ?  ','+clustering_key.join(",") : '';

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
        keyspace = this._properties.cql.options.keyspace;

    var query = "SELECT * FROM system.schema_columns WHERE columnfamily_name = ? AND keyspace_name = ? ALLOW FILTERING;";

    this._properties.cql.execute(query,[table_name,keyspace], function(err, result) {

        if (err) return callback(build_error('model.tablecreation.dbschemaquery', err));

        if(!result.rows || result.rows.length === 0)
            return callback();

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
BaseModel._execute_table_query = function(query, consistency, callback){
    
    var do_execute_query = function(doquery,docallback){
        this.execute_query(doquery, consistency, docallback);
    }.bind(this,query);

    if(this.is_table_ready()){
        do_execute_query(callback);
    }
    else{
        this.init(function(){
            do_execute_query(callback);
        });
    }

};

/**
 * Given a field name and a value, format the query portion regarding that value
 * @param  {string} fieldname  Name of the filed
 * @param  {string} fieldvalue Value of the filed
 * @return {string}            String to be used in query
 * @protected
 */
BaseModel._get_db_value_expression = function(fieldname,fieldvalue){
    var properties = this._properties,
        schema = properties.schema,
        fieldtype = schema.fields[fieldname];

    if(fieldvalue instanceof Array){
        var val = fieldvalue.map(function(v){
                return this._get_db_value_expression(fieldname, v);
            }.bind(this)).join(', ');
        return util.format('(%s)',val);
    }

    if (fieldtype == 'text')                
        return util.format("'%s'",fieldvalue);
    else if(fieldvalue instanceof Date)
        return util.format("'%s'",fieldvalue.toISOString().replace(/\..+/, ''));
    //blob data are passed through strings
    else if(fieldtype == 'blob')
        return util.format("textAsBlob('%s')",fieldvalue.toString());
    else
        return fieldvalue;
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
        this._drop_table(function(err){
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
BaseModel.execute_query = function(query, consistency, callback){
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
        raw : false
    };

    options = lodash.defaults(options, defaults);

    var query;
    try{
        query = this._create_find_query(query_ob, options);
    }
    catch(e){
        return callback(e);
    }

    this._execute_table_query(query, CONSISTENCY_FIND, function(err,results){
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

    var defaults = {};

    options = lodash.defaults(options, defaults);

    var query = 'DELETE FROM %s %s',
        where = '';
    try{
        where = this._create_where_clause(query_ob, options);
    }
    catch(e){
        return callback(e);
    }
    query = util.format(query, this._properties.table_name, where);
    this._execute_table_query(query, CONSISTENCY_DELETE, function(err,results){
        if(err) return callback(build_error('model.find.dberror',err));
        callback();
    });

};


/* Instance Public --------------------------------------------- */

/**
 * Save this instance of the model
 * @param  {object}                     [options={}] - options for the query
 * @param  {BaseModel~QueryExecution}   callback - Result of the save or an error eventually
 * @instance
 */
BaseModel.prototype.save = function(options, callback){
    if(arguments.length == 1){
        callback = options;
        options = undefined;
    }
    callback = callback || noop;

    var identifiers = [], values = [],
        properties = this.constructor._properties,
        schema = properties.schema;

    for(var f in schema.fields){
        var fieldtype = schema.fields[f],
            fieldvalue = this[f],
            fieldvalidator = TYPE_MAP[fieldtype].validator;

        if(!fieldvalue){
            if(schema.key.indexOf(f) < 0 && schema.key[0].indexOf(f) < 0)
                continue;
            else
                return callback(build_error('model.save.unsetkey',f));
        }

        if(!fieldvalidator(fieldvalue)){
            return callback(build_error('model.save.invalidvalue',fieldvalue,f,fieldtype));
        }

        identifiers.push(f);

        values.push(this.constructor._get_db_value_expression(f,fieldvalue));
    }

    var query = util.format(
        "INSERT INTO %s ( %s ) VALUES ( %s )",
        properties.qualified_table_name,
        identifiers.join(" , "),
        values.join(" , ")
    );
    
    this.constructor._execute_table_query(query,CONSISTENCY_SAVE,callback);
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
*/