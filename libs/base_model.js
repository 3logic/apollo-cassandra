
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

module.exports = BaseModel;