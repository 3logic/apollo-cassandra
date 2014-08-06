//libreria di cassandra

var cql = require("node-cassandra-cql");
var check = require('check-types');
var async = require('async');
var querystring = require("querystring");
var util = require("util");
var BaseModel = require('./base_model');
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



/**
 * Utilità per cassandra
 * @param {Apollo~Configuration} configuration configurazione di Apollo
 * @class
 */
var Apollo = function(connection, options){
    if(!connection) throw "Data connection configuration undefined";

    this._options = options | {};
    this._models = {};
    this._keyspace = connection.keyspace;
    
    //impostazione della connessione che viene esguita alla prima esecuzione
    this._client = new cql.Client(connection);

    //this._client.on("log", console.log);
};


/**
 * Funzione statica per assicurare creazione del keyspace indicato in una connessione
 * 
 * @param  {Apollo~connection}   connection [description]
 * @param  {Function} callback   [description]
 */
Apollo.assert_keyspace = function(connection, callback){
    var copy_fields = ['hosts'],
        temp_connection = {};

    for(var fk in copy_fields){
        temp_connection[copy_fields[fk]] = connection[copy_fields[fk]]
    }

    var keyspace_name = connection.keyspace,
        client = new cql.Client(connection);

    var query = util.format(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
        keyspace_name
    );

    client.execute(query, function(err,result){
        client.shutdown();
        callback(err,result)
    });
}



Apollo.prototype = {

    is_integer : function (obj){
        return check.intNumber(obj);
    },

    is_boolean : function (obj){
        return obj === true || obj === false;
    },

    is_number : function (obj){
        return check.number(obj);
    },

    is_string : function (obj){
        return check.string(obj);
    },    

    is_datetime : function (obj){
        return check.date(obj);
    },

    is_tablename : function (obj){
        return (check.string(obj) && /^[a-z]+[a-z0-9_]*/.test(obj) ); 
    },

    _TYPE_MAP : {    
        bigint : {validate : this.is_integer, dbvalidator : "org.apache.cassandra.db.marshal.LongType"},
        blob : {validate : function(){return true;}, dbvalidator : "org.apache.cassandra.db.marshal.BytesType"},
        boolean : {validate : this.is_boolean, dbvalidator : "org.apache.cassandra.db.marshal.BooleanType"},        
        decimal   : {validate : this.is_number, dbvalidator : "org.apache.cassandra.db.marshal.DecimalType"},        
        double    : {validate : this.is_number, dbvalidator : "org.apache.cassandra.db.marshal.DoubleType"},
        float     : {validate : this.is_number, dbvalidator : "org.apache.cassandra.db.marshal.FloatType"},
        int   : {validate : this.is_integer, dbvalidator : "org.apache.cassandra.db.marshal.Int32Type"},
        text      : {validate : this.is_string, dbvalidator : "org.apache.cassandra.db.marshal.UTF8Type"},
        timestamp  : {validate : this.is_datetime, dbvalidator : "org.apache.cassandra.db.marshal.TimestampType"},        
        varint   : {validate : this.is_integer, dbvalidator : "org.apache.cassandra.db.marshal.IntegerType"}
    },

    _get_type_from_validator : function(val){
        for(var t in this._TYPE_MAP){            
            if (this._TYPE_MAP[t].dbvalidator == val)
                return t;
        }
        return null;
    },

   
    _generate_model : function(properties){

        var Model = function(instance_values){
           BaseModel.apply(this,Array.prototype.slice.call(arguments));           
        }

        util.inherits(Model,BaseModel);

        for(var i in BaseModel){
            if(BaseModel.hasOwnProperty(i)){
               Model[i] = BaseModel[i];
            }
        }
        Model._properties = properties;

        return Model;
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
            if (!(model_schema.fields[k] in this._TYPE_MAP))
                throw("Tipo di field non riconosciuto, colonna: " + k);
        }

        for(var i in model_schema.key){
            if((typeof(model_schema.key[i]) != "string") || !(model_schema.key[i] in model_schema.fields))
                throw("Key deve essere un array di nomi di colonna");
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
    add_model : function(model_name, model_schema) {
        if(!model_name || typeof(model_name) != "string")
            throw("Si deve specificare un nome per il modello");    

        this.validate_model_schema(model_schema);

        var table_name = model_schema.table_name || model_name;
        if(!this.is_tablename(table_name))
            throw("Nomi tabella: caratteri validi alfanumerici ed underscore, devono cominciare per lettera");  

        var properties = {
            name : model_name,
            schema : model_schema,
            table_name : table_name,
            cql : this._client
        };

        return this._models[model_name] = this._generate_model(properties);
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