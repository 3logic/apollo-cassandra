//libreria di cassandra

var cql = require("node-cassandra-cql");
var async = require('async');
var querystring = require("querystring");
var util = require("util");
var BaseModel = require('./base_model');
var TYPE_MAP = require('./cassandra_types');


/**
 * Utilità per cassandra
 * @param {Apollo~Configuration} configuration configurazione di Apollo
 * @param {Apollo~CassandraOptions} options - Cassandra options
 * @class
 */
var Apollo = function(connection, options){
    if(!connection) throw "Data connection configuration undefined";

    this._options = options || { 
        replication : {'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    };
    this._models = {};
    this._keyspace = connection.keyspace;
    this._connection = connection;
    
    this._client = null;
};



Apollo.prototype = {

    is_tablename : function (obj){
        return ( typeof obj == 'string' && /^[a-z]+[a-z0-9_]*/.test(obj) ); 
    },
   
    _generate_model : function(properties){

        var Model = function(instance_values){
           BaseModel.apply(this,Array.prototype.slice.call(arguments));           
        };

        util.inherits(Model,BaseModel);

        for(var i in BaseModel){
            if(BaseModel.hasOwnProperty(i)){
               Model[i] = BaseModel[i];
            }
        }
        Model._properties = properties;

        return Model;
    },

    /**
      * Ensure specified keyspace exists, try to create it otherwise
      * @param  {Apollo~GenericCallback} callback Called on keyspace assertion
      */
    _assert_keyspace : function(callback){
        var copy_fields = ['hosts'],
            temp_connection = {},
            connection = this._connection,
            options = this._options;

        for(var fk in copy_fields){
            temp_connection[copy_fields[fk]] = connection[copy_fields[fk]];
        }

        var keyspace_name = connection.keyspace,
            client = new cql.Client(temp_connection);

        var replication_text = '';
        switch(options.replication.class){
            case 'SimpleStrategy':
                replication_text = util.format("{ 'class' : 'SimpleStrategy', 'replication_factor' : %d}", options.replication.replication_factor );
                break;
            default:
                replication_text = "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1}";
                break;

        }

        var query = util.format(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = %s;",
            keyspace_name,
            replication_text
        );
        client.execute(query, function(err,result){
            client.shutdown();
            callback(err,result);
        });
    },


    /**
     * Connect your instance of Apollo to Cassandra
     * @param  {Apollo~onConnect} callback Callback on connection result
     */
    connect : function(callback){
        var on_keyspace = function(err){
            if(err){ return callback(err);}
            this._client = new cql.Client(this._connection);
            callback(null, this);
        };

        if(this._keyspace){
            this._assert_keyspace( on_keyspace.bind(this) );
        }else{
            on_keyspace.call(this);
        }
    },


    /*
        descrittore model:
        {
            fields : { //obbligatorio
                column1 : "tipo",
                column2 : "tipo2",
                column3 : "tipo3"
            },
            key : ["column1","column2"],
            indexes : ["column1","column3"] 
        }
     */

    validate_model_schema: function(model_schema){
        if(!model_schema)
            throw("Si deve specificare uno schema del modello");

        if(typeof(model_schema.fields) != "object" || Object.keys(model_schema.fields).length === 0 )
            throw("Schema deve contenere una mappa fields non vuota");
        if(!model_schema.key)
            throw("Si deve specificare la chiave del modello");
        if(!(model_schema.key instanceof Array))
            throw("Key deve essere un array di nomi di colonna");

        for( var k in model_schema.fields){
            if (!(model_schema.fields[k] in TYPE_MAP))
                throw("Tipo di field non riconosciuto, colonna: " + k);
        }

        if( typeof(model_schema.key[0]) == "string" ){
            if(!(model_schema.key[0] in model_schema.fields)) 
                throw("La partition key deve essere un nome di colonna");
        }
        else if(model_schema.key[0] instanceof Array){
            for(var j in model_schema.key[0]){
                if((typeof(model_schema.key[0][j]) != "string") || !(model_schema.key[0][j] in model_schema.fields))
                        throw("La partition key multipla deve essere un array di nomi di colonna");
            }
        }
        else {
            throw("La partition key deve essere una stringa o un array di nomi di colonna");
        }
        
        for(var i in model_schema.key){
            if(i>0){
                if((typeof(model_schema.key[i]) != "string") || !(model_schema.key[i] in model_schema.fields))
                    throw("Key deve essere un array di nomi di colonna");
            }

        }

        if(model_schema.indexes){
            if(!(model_schema.indexes instanceof Array))
                throw("indexes deve essere un array di nomi di colonna");
            for(var j in model_schema.indexes)
                if((typeof(model_schema.indexes[j]) != "string") || !(model_schema.indexes[j] in model_schema.fields))
                    throw("indexes deve essere un array di nomi di colonna");
        }
    },

    /**
     * Aggiunge un modello a quelli conosciuti
     * @param {string}  model_name         Nome  del modello
     * @param {obj}     model_schema      schema del modello (in formato definito)
     */
    get_model : function(model_name, model_schema, options) {
        if(!model_name || typeof(model_name) != "string")
            throw("Si deve specificare un nome per il modello");    

        options = options || {};
        options.mismatch_behaviour = options.mismatch_behaviour || 'fail';
        if(options.mismatch_behaviour !== 'fail' && options.mismatch_behaviour !== 'drop')
            throw 'Valid option values for "mismatch_behaviour": "fail" , "drop". Got: "'+options.mismatch_behaviour+'"';

        if(typeof model_schema.key[0] === 'string'){
            model_schema.key[0] = [model_schema.key[0]];
        }

        this.validate_model_schema(model_schema);

        var table_name = model_schema.table_name || model_name;
        if(!this.is_tablename(table_name))
            throw("Nomi tabella: caratteri validi alfanumerici ed underscore, devono cominciare per lettera");  

        var qualified_table_name = this._client.options.keyspace+'.'+table_name;

        var properties = {
            name : model_name,
            schema : model_schema,
            table_name : table_name,
            qualified_table_name: qualified_table_name,
            cql : this._client,
            mismatch_behaviour : options.mismatch_behaviour
        };

        return (this._models[model_name] = this._generate_model(properties));
    },


    /**
     * Stringa di update da utilizzare in PIG nel formato:
     * ‘cql://myschema/example?output_query=update example set value1 @ #,value2 @ #’ 
     *
     * @param  {string} model_name Nome del modello precedentemente registrato
     * @param  {bool} encode     Indica che se la strigna deve essere in URL encode o meno
     * @return {string}            Strigna per PIg
     */
    pig_cql_update_connection : function(model_name, encode, callback){
        if ( !(model_name in this._models) || !this._models[model_name])
            return callback("Modello non conosciuto");

        //prima parte delal stringa
        var cqlstring = "cql://" + this._keyspace + "/" + model_name + "?";
        //inizio update
        var cqlquerystring = {output_query: "update " + this._keyspace + "." + model_name + " set "};

        //aggiunta delle colonne
        var values = [];
        for (var f in this._models[model_name].fields)
            values.push(f + " @ #");
        cqlquerystring.output_query += values.join(" , ");
        //inserimento della seconda parte in versione encode o meno a seconda della richeista
        if (encode)
            cqlstring +=  querystring.stringify(cqlquerystring);
        else
            cqlstring += "output_query=" + cqlquerystring.output_query;
        return callback(null,cqlstring);

    },

    /**
     * Connessione per lettura da cassandra in pig
     * @param  {string} keyspace Keyspace della tabella
     * @param  {string} table    nome della tabella
     * @return {string}          stringa di connessione
     */
    pig_cql_connection : function(keyspace,table){
        var cqlstring = "cql://" + keyspace + "/" + table + "?";       
        return cqlstring;
    },

    /**
     * Chiusura della connessione
     * @param  {Function} callback callback
     */
    close : function(callback){        
        this._client.shutdown(callback);
    }
};

module.exports = Apollo;

/**
 * Generic callback with just error parameter.
 * @callback Apollo~GenericCallback
 * @param {object} err
 */

/**
 * This callback is displayed as part of the Apollo class.
 * @callback Apollo~onConnect
 * @param {object} err
 */

/**
 * Opzioni per il client cassandra
 * @typedef {Object} Apollo~CassandraOptions
 * @property {object} replication - replication configuration object
 */

 /**
  * Opzioni per il client cassandra
  * @typedef {Object} Apollo~Configuration
  * @property {Apollo~connection} connection - Configurazione per la connessione client cassandra
  */

 /**
  * Opzioni per la connessione client cassandra
  * @typedef {Object} Apollo~connection
  * @property {array} hosts - Array of string in host:port format. Port is optional (default 9042).
  * @property {string} keyspace - Name of keyspace to use.
  * @property {string} [username=null] - User for authentication.
  * @property {string} [password] - Password for authentication.
  */
