Apollo
======

[![Build Status](https://travis-ci.org/3logic/apollo-cassandra.svg?branch=master)](https://travis-ci.org/3logic/apollo-cassandra)
[![Coverage Status](https://coveralls.io/repos/3logic/apollo/badge.png?branch=master)](https://coveralls.io/r/3logic/apollo?branch=master)


Apollo is a <a href="http://cassandra.apache.org/" target="_blank">Cassandra</a> object modeling for <a href="http://nodejs.org/" target="_blank">node.js</a>

## Notes

*Apollo is in early develeopment stage. Code and documentation are incomplete!*


## Installation

`npm install --save apollo-cassandra`

## Usage

Include Apollo and start creating your models

```javascript
var Apollo = require('apollo-cassandra');

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

`Apollo` constructor takes two arguments: `connection` and `options`. Let's see what they are in depth:

- `connection` are a set of options for your connection and accept the following parameters:
    
    - `hosts` is an array of string written in the form `host:port` or simply `host` assuming that the default port is 9042
    - `keyspace` is the keyspace you want to use. If it doesn't exist apollo will create it for you
    - `username` and `password` are used for authentication
    - Any other parameter is defined in [api](#api)

- `options` are a set of generic options. Accept the following parameters:
    
    - `replication_strategy` can be an object or a string representing <a href="http://www.datastax.com/documentation/cassandra/2.0/cassandra/architecture/architectureDataDistributeReplication_c.html" target="_blank">cassandra replication strategy</a>. Default is `{'class' : 'SimpleStrategy', 'replication_factor' : 1 }`

Here is a complete example: 

```javascript
var apollo = new Apollo(
    {
       hosts: ['1.2.3.4', '12.3.6.5', 'cassandra.me.com:1212'],
       keyspace: 'mykeyspace',
       username: 'username',
       password: 'password'
    },
    {
        replication_strategy: {'class' : 'NetworkTopologyStrategy', 'dc1': 2 }
    }
);
```

## Schema

Now that apollo is connected, create a `model` describing it through a `schema`

```javascript
var personSchema = {
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

Now create a new Model based on your schema. The function `add_model` uses the `table name` and `schema` as parameters.

```javascript
var Person = apollo.add_model('person',personSchema);
```

From your model you can query cassandra or save your instances

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

A schema can be a complex object. Take a look at this example

```javascript
personSchema = {
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

What does the above code means?
- `fields` are the columns of your table. For each column name the value can be a string representing the type or an object containing more specific informations. i.e.
    + ` "id"     : { "type": "uuid", "default": {"$db_function": "uuid()"} },` in this example id type is `uuid` and the default value is a cassandra function (so it will be executed from the database). 
    + `"name"   : { "type": "varchar", "default": "no name provided"},` in this case name is a varchar and, if no value will be provided, it will have a default value of `no name provided`. The same goes for `surname`.
    + `complete_name` the default values is calculated from others field. When apollo processes you model instances, the `complete_name` will be the result of the function you defined. In the function `this` is bound to the current model instance.
    + `age` no default is provided and we could write it just as `"age": "int"`.
    + `created`, like uuid(), will be evaluated from cassandra using the `now()` function.
- `key`: here is where you define the key of your table. As you can imagine, the first value of the array is the `partition key` and the others are the `clustering keys`. The `partition key` can be an array defining a `compound key`. Read more about keys on the <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_using/use_compound_keys_t.html" target="_blank">documentation</a>
- `indexes` are the index of your table. It's always an array of field names. You can read more on the <a href="http://www.datastax.com/documentation/cql/3.1/cql/ddl/ddl_primary_index_c.html" target="_blank">documentation</a>

## Generating your model

A model is an object representing your cassandra `table`. Your application interact with cassandra through your models. An instance of the model represents a `row` of your table.

Let's create our first model
```javascript
var Person = apollo.add_model('person',personSchema);
```

now instantiate a person
```javascript
var john = new Person({name: "John", surname: "Doe"});
```

When you instantiate a model, every field you defined in schema is automatically a property of your instances. So, you can write:

```javascript
john.age = 25;
console.log(john.name); //John
console.log(john.complete_name); // undefined.
```
__note__: `john.complete_name` is undefined in the newly created instance but will be populated when the instance is saved because it has a default value in schema definition

John is a well defined person but he is not still persisted on cassandra. To persist it we need to save it. So simply:

```javascript
john.save(function(err){
    if(err)
        return 'Houston we have a problem';
    else
        return 'all ok, saved :)';

});
```

When you save an instance all internal validators will check you provided correct values and finally will try to save the instance on cassandra.

Ok, we are done with John, let's delete it:

```javascript
john.delete(function(err){
    //...
});
```

ok, goodbye John.

### A few handy tools for your model

Apollo instances provide some utility methods. To generate uuids e.g. in field defaults:

*   `apollo.uuid()`  
    returns a type 3 (random) uuid, suitable for Cassandra `uuid` fields, as a string
*   `apollo.timeuuid()`  
    returns a type 1 (time-based) uuid, suitable for Cassandra `timeuuid` fields, as a string

## Virtual fields

Your model could have some fields which are not saved on database. You can define them as `virtual`

```javascript
personSchema = {
    "fields": {
        "id"     : { "type": "uuid", "default": {"$db_function": "uuid()"} },
        "name"   : { "type": "varchar", "default": "no name provided"},
        "surname"   : { "type": "varchar", "default": "no surname provided"},
        "complete_name" : {
            "type": "varchar",
            "virtual" : {
                get: function(){return this.name + ' ' +this.surname;},
                set: function(value){
                    value = value.split(' ');
                    this.name = value[0];
                    this.surname = value[1];
                }
            }
        }
    }
}
```

A virtual field is simply defined adding a `virtual` key in field description. Virtuals can have a `get` and a `set` function, both optional (you should define at least one of them!).
`this` inside get and set functions is bound to current instance of your model.

## Validators

Every time you set a property for an instance of your model, an internal type validator checks that the value is valid. If not an error is thrown. But how to add a custom validator? You need to provide your custom validator in the schema definition. For example, if you want to check age to be a number greater than zero:

```javascript
var personSchema = {
    //... other properties hidden for clarity
    age: {
        type : "int",
        rule : function(value){ return value > 0; }
    }
}
```

your validator must return a boolean. If someone will try to assign `john.age = -15;` an error will be thrown.
You can also provide a message for validation error in this way

```javascript
var personSchema = {
    //... other properties hidden for clarity
    age: {
        type : "int",
        rule : {
            validator : function(value){ return value > 0; },
            message   : 'Age must be greater than 0'
        }
    }
}
```

then the error will have your message. Message can also be a function; in that case it must return a string:

```javascript
var personSchema = {
    //... other properties hidden for clarity
    age: {
        type : "int",
        rule : {
            validator : function(value){ return value > 0; },
            message   : function(value){ return 'Age must be greater than 0. You provided '+ value; }
        }
    }
}
```

The error message will be `Age must be greater than 0. You provided -15`

Note that default values _are_ validated if defined either by value or as a javascript function. Defaults defined as DB functions, on the other hand, are never validated in the model as they are retrieved _after_ the corresponding data has entered the DB.
If you need to exclude defaults from being checked you can pass an extra flag:

```javascript
var blogUserSchema = {
    //... other properties hidden for clarity
    email: {
        type : "text",
        default : "<enter your email here>",
        rule : {
            validator : function(value){ /* code to check that value matches an email pattern*/ },
            ignore_default: true
        }
    }
}
```

## Querying your data

Ok, now you have a bunch of people on db. How do I retrieve them?

### Find

```javascript
Person.find({name: 'John'}, function(err, people){
    if(err) throw err;
    console.log('Found ', people);
});
```

In the above example it will perform the query `SELECT * FROM person WHERE name='john'` but `find()` allows you to perform even more complex queries on cassandra.  You should be aware of how to query cassandra. Every error will be reported to you in the `err` argument, while in `people` you'll find instances of `Person`. If you don't want apollo to cast results to instances of your model you can use the `raw` option as in the following example:

```javascript
Person.find({name: 'John'}, { raw: true }, function(err, people){
    //people is an array of plain objects
});
```

Let's see a complex query

```javascript
var query = {
    name: 'John', // stays for name='john' 
    age : { '$gt':10 }, // stays for age>10 You can also use $gte, $lt, $lte
    surname : { '$in': ['Doe','Smith'] }, //This is an IN clause
    $orderby:{'$asc' :'age'} }, //Order results by age in ascending order. Also allowed $desc and complex order like $orderby:{'$asc' : ['k1','k2'] } }
    $limit: 10 //limit result set

}
```

Note that all query clauses must be Cassandra compliant. You cannot, for example, use $in operator for a key which is not the partition key. Querying in Cassandra is very basic but could be confusing at first. Take a look at this <a href="http://mechanics.flite.com/blog/2013/11/05/breaking-down-the-cql-where-clause/" target="_blank">post</a> and, obvsiouly, at the <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_using/about_cql_c.html" target="_blank">documentation</a>


## API

Complete API definition is available on the <a href="http://apollo.3logic.it" target="_blank">3logic website</a>.
Anyway, you can generate the documentation by cloning this project and launching `grunt doc`

## Test

To test Apollo create a file named `local_conf.json` in `test` directory with your connection configuration as below

```json
{
    "contactPoints": [
       "127.0.0.1",
       "192.168.100.65",
       "my.cassandra.com:9845"
    ],
    "keyspace": "tests"
}
```

## About

Apollo is brought to you by

- [Niccolò Biondi](https://github.com/bionicco)
- [Elia Cogodi](https://github.com/ecogodi)
- [Fabrizio Ruggeri](https://github.com/ramiel)

Thanks to _Gianni Cossu_ and [Massimiliano Atzori](https://github.com/amaxis) for helping.

Thanks to [3logic](http://www.3logic.it) too!
