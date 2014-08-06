//libreria di cassandra

var cql = require("node-cassandra-cql");
var check = require('check-types');
var async = require('async');
var querystring = require("querystring");
var util = require("util");

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


var BaseModel = function(instance_values){
    var _fields = {};
    var fields = this.constructor._properties.schema.fields;
    
    for(var fields_keys = Object.keys(fields), i = 0, len = fields_keys.length; i < len; i++){
        var property_name = fields_keys[i];
        var descriptor = {
            enumerable: true,
            set : function(prop_name, new_value){
                _fields[prop_name] = new_value;
            }.bind(null, property_name),
            get: function(prop_name){
                return _fields[prop_name];
            }.bind(null, property_name)
        };
        Object.defineProperty(this, property_name, descriptor);
        this[property_name] = instance_values[property_name];
    }

};

/* Static Private ---------------------------------------- */

BaseModel._properties = {
    name : null,
    schema : null
}

BaseModel._INFO_TABLE_QUERY = "SELECT * FROM system.schema_columns WHERE columnfamily_name = ? ALLOW FILTERING;";

BaseModel._create_table = function(callback){
    var properties = this._properties,
        table_name = properties.table_name,
        model_schema = properties.schema;

    //controllo esistenza della tabella ed eventuale corrispondenza con lo schema
    this._get_table_schema(table_name,function(err,db_schema){
        if(err) return callback(err);            
        if (db_schema){//controllo se uguali            
            if (!this._schema_compare(model_schema, db_schema))
                return callback("Lo schema collide con la tabella esistente"); 
            else callback();               
        }
        else{    //se non esiste viene creata                
            this._client.execute(this._create_table_query(table_name,model_schema), function(err, result) {
                if (err) return callback('Fallimento creazione tabella ', err);   
                //creazione indici  
                if(model_schema.indexes instanceof Array)
                    async.each(model_schema.indexes, function(idx,next){
                        this._client.execute(this._create_index_query(table_name,idx), function(err, result) {
                            if (err) return next('Fallimento creazione indice ', err); 
                            next();
                        });
                    }.bind(this),callback);
                else
                    callback();
            }.bind(this));
        }
    }.bind(this));
};


//crea query con parametri da riempire
BaseModel._create_table_query = function(table_name,schema){
    //creazione tabella
    var query = "CREATE TABLE IF NOT EXISTS  \"" + table_name + "\" (";
    var rows = [];
    for(var k in schema.fields)
        rows.push(k + " " + schema.fields[k]);
    query += rows.join(" , ");

    if(schema.key)
        query += " , PRIMARY KEY ((" + schema.key.join(",") + "))";

    query +=" ); ";

    return query;
};


//crea query per aggiunta indice
BaseModel._create_index_query = function(table_name, index_name){
    var query = utils.format(
        "CREATE INDEX IF NOT EXISTS ON %s (%s);", 
        table_name, 
        index_name
    );
    return query;
};


//recupera lo schema della tabella, se la tabella non esiste è null
BaseModel._get_table_schema = function (table_name, callback){
    var table_qualified_name = this._properties.cql.keyspace+'.'+table_name;

    this._properties.cql.execute(this._INFO_TABLE_QUERY,[table_qualified_name], function(err, result) {
        if (err) return callback('Errore durante analisi schema tabella: '+err);
        if(!result.rows || result.rows.length === 0)
            return callback();

        var db_schema = {fields:{}};
        for(var r in result.rows){
            var row = result.rows[r];
            db_schema.fields[row.column_name] = this._get_type_from_validator(row.validator);                
            if(row.type == 'partition_key'){
                if(!db_schema.key)
                    db_schema.key = [];
                db_schema.key.push(row.column_name);
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

BaseModel._schema_compare = function(schema1,schema2){
    return this._schema_compare_inner(schema1,schema2) && this._schema_compare_inner(schema2,schema1);
};

BaseModel._schema_compare_inner = function(schema1,schema2){
    if( (typeof(schema1) != typeof(schema2) ) && ( (schema1 === null) != (schema2 === null) ) )
        return false;

    for (var p in schema1){
        //array
        if(schema1[p] instanceof Array){
            //controllo che sia array e della stessa lunghezza
            if(!(schema2[p] instanceof Array) || schema1[p].length != schema2[p].length)
                return false;
            //ciclo elementi per vedere se esistono in entrambi
            for(var i in schema1[p]){
                var found = false;
                for(var j in schema2[p])
                    if(this._schema_compare(schema2[p][j],schema1[p][i])){
                        found = true; break;
                    }
                if(!found)
                    return false;
            }
        }
        //oggetti (ricorsivo)
        else if(typeof(schema1[p]) == "object"){
            if(!this._schema_compare(schema1[p],schema2[p]))
                return false;
        }else{
            if(schema1[p] !== schema2[p])
                return false;
        }
    }
    return true;
};


/* Static Public ---------------------------------------- */

BaseModel.find = function(filter_ob, options, callback){
    console.log('find ', arguments, this._properties);
};


BaseModel.prototype.save = function(options, callback){
    var identifiers = [], values = [],
        properties = this.constructor._properties,
        schema = properties.schema;
    
    for(var f in schema.fields){
        var fieldtype = schema.fields[f],
            fieldvalue = this[f],
            fieldvalidator = _TYPE_MAP.fieldtype.validator;

        if(!fieldvalue){
            if(schema.key.indexOf(f) < 0)
                continue;
            else
                return callback("La chiave deve essere valorizzata nell'oggetto");
        }

        if(!fieldvalidator(fieldvalue)){
            return callback(utils.format(
                "Valore %s non valido per Campo %s (Tipo %s)",
                fieldvalue,
                f,
                fieldtype
            ));
        }

        identifiers.push(f);
        //if(fieldtypeof(fieldvalue) == 'string')
        if (fieldtype == 'text')                
            //stringhe fra apici
            values.push("\'" + fieldvalue + "\'");
        else if(fieldvalue instanceof Date)
            //date fra apici e in formato "yyyy-mm-dd'T'HH:mm:ss"              
            values.push("\'" + fieldvalue.toISOString().replace(/\..+/, '') + "\'");
        
        //i dati blob vengono passati come stringhe
        else if(fieldtype == 'blob')
            values.push(" textAsBlob(\'" + fieldvalue.toString() + "\')");
        else
            values.push(fieldvalue);
    }

    var table_qualified_name = properties.cql.keyspace+'.'+properties.table_name;
    
    var query = "INSERT INTO " + table_qualified_name + " ( ";
        query += identifiers.join(" , ") + " ) ";
        query += " VALUES ( " + values.join(" , ") + " ) ";
       
    this._properties.cql.execute(query, function(err, result) {
        if(err) return callback(err);
        callback();
    });
}


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
    this._properties.cql = new cql.Client(connection);

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
        "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };",
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

        Model.create_table = this._create_table.bind(this, properties.table_name, properties.schema);

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

        validate_model_schema(model_schema);

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