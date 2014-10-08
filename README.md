Apollo
======

[![Build Status](https://travis-ci.org/3logic/apollo.svg?branch=master)](https://travis-ci.org/3logic/apollo)


Apollo is a <a href="http://cassandra.apache.org/" target="_blank">Cassandra</a> object modeling for <a href="http://nodejs.org/" target="_blank">node.js</a>

##Notes
*Apollo is in early develeopment stage. Code and documentation are incomplete!*


##Installation

`npm install apollo`

##Usage

Include Apollo and start creating your models

```javascript
var Apollo = require('apollo');

var connection = {
    "hosts": [
        "127.0.0.1"
    ],
    "keyspace": "my_keyspace"
};

var apollo = new Apollo(connection);
apollo.connect(function(err){
    if(err) throw err;
    /* do amazing things! */
})
```

### Connection

`Apollo` constructor takes two arguments: `connection` and `options`. Let's see what tey are in depth:

- `connection` are a set of options for your connection and accept the following parameters:
    
    - `hosts` is an array of string written in the form `host:port` or simply `host` assuming that the default port is 9042
    - `keyspace` is the keyspace you want to use. If it doesn0t exists apollo will create it for you
    - `username` and `password` are used for authentication
    - Any other parameter is defined in [api](#api)

- `options` are a set of generic options. Accept the following parameters:
    
    - `replication_strategy` can be an object or a string representing <a href="http://www.datastax.com/documentation/cassandra/2.0/cassandra/architecture/architectureDataDistributeReplication_c.html" target="_blank">cassandra replication strategy</a>.Default is 

## Schema

Now that apollo is connected start create your models.
To create a model just start describe it through a schema

```javascript
var PersonSchema = {
    { 
        fields:{
            name    : "text",
            surname : "text",
            age     : "int"
        }, 
        key:["name"] 
    }
};
```

Now create a new Model based on your schema. The function `add_model` take the table name and the schema.

```javascript
var Person = apollo.add_model('person',PersonSchema);
```

Through your model you can query db or save your instances

```javascript
/*Quesry your db*/
Person.find({name: 'jhon'}, function(err, people){
    if(err) throw err;
    console.log('Found ', people);
});

/*Save your instances*/
var alex = new Person({name: "Alex", surname: "Rubiks", age: 32});
alex.save(function(err){
    if(!err)
        console.log('Yuppiie!');
});
```


## Schema in detail

A schema can be a complex object. Take a look to this

```javascript
PersonSchema = {
    "fields": {
        "id"     : { "type": "uuid", "default": {"$db_function": "uuid()"} },
        "name"   : { "type": "varchar", "default": "no name provided"},
        "surname"   : { "type": "varchar", "default": "no surname provided"},
        "complete_name" : { "type": "varchar", "default": function(){ return this.name + ' ' + this.surname;}},
        "age"    :  { "type": "int" },
        "created"     : {"type": "timestamp", "default" : {"$db_function": "now()"} }
    },
    "key" : [["id"],"created"],
    "indexes": ["name"]
}
```

This is a complex schema Let describe it:
- `fields` are the fields of your table. Even if you can specify them indicating only the type we can do more. i.e 
    + ` "id"     : { "type": "uuid", "default": {"$db_function": "uuid()"} },` is indicating that id is of type `uuuid` and the default values is a cassandra function (so it will be executed from the database) and tit is `uuuid()`. 
    + `"name"   : { "type": "varchar", "default": "no name provided"},` in this case name is a varchar and, if no value will be provided, it will have a default value of `no name provided`. The same is for `surname`
    + `complete_name` is a value for whuich the default values is calculated starting from others field. When apollo processes you model instances, the `complete_name` will be the result of the function you defined. In the function `this` is the current model instance.
    + `age` if of type `int`. No default is provided and actually we could write it as `"age": "int"`
    + `created`, like uuid, will be evalueted from db usgin the `now()` function
- `key`: here is where you define the key of your table. As you can imagine, the first value of the array is the `partition key` and the others are the `clustering keys`. You can use an array as first value to use define a `compound key`. Read more about keys <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_compound_keys_t.html" target="_blank">here</a>
- `indexes` are the index of your table. It's always an array of fields. You can read more <a href="http://www.datastax.com/documentation/cql/3.1/cql/ddl/ddl_primary_index_c.html" target="_blank">here</a>

Schema will soon support custom validators

## Generating your model

As previous seen you can generate your model. A model is an object representing your cassandra `table`. Your application interact with cassandra trough your models. An instance of the model represents a `row` of your table.

Let's create our first model
```javascript
var Person = apollo.add_model('person',PersonSchema);
```

now instantiate a person
```javascript
var john = new Person({name: "John", surname: "Doe"});
```

When you instantiate a model, every field you defined in schema is automatically a property of your instances. So, you can write

```javascript
john.age = 25;
console.log(john.complete_name); //Jhon Doe
```
__note__: this is not completely true at the moment :)

John is a well defined person but he is not still persisted on cassandra. To persist it we need to save. So simple:

```javascript
john.save(function(err){
    if(err)
        return 'huston we have a problem';
    else
        return 'all ok, saved :)';

});
```

When you save an instance all internal validators will check you provided correct values and finally will try to save the instance on cassandra.

Ok, we have done with john, let's delete it:

```javascript
john.delete(function(err){
    //...
});
```

ok, goodbye jhon.

## Querying your data

Ok, now you have a bunch of people on db. How to retrieve them?
Your model have a caouple of static function.

### find

```javascript
Person.find({name: 'jhon'}, function(err, people){
    if(err) throw err;
    console.log('Found ', people);
});
```

find let you perform complex query on cassandra. This will be `SELECT * FROM person WHERE name='jhon'`. You should be aware of how to query cassandra. Every error will be reported to you in the `err` argument, while in `people` you'll find instances of `Person`


## Api

Complete api definition will be public available soon. Meanwhile you can generate documentation cloning this project and launching `grunt doc`

## About

Apollo is brought to you by
- [Niccolò Biondi](https://github.com/bionicco)
- [Elia Cogodi](https://github.com/ecogodi)
- [Fabrizio Ruggeri](https://github.com/ramiel)

and with the support of [3logic](http://www.3logic.it)
