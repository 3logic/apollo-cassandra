apollo
======

[![Build Status](https://travis-ci.org/3logic/apollo.svg?branch=master)](https://travis-ci.org/3logic/apollo)


Apollo is a [Cassandra](http://cassandra.apache.org/) object modeling for [node.js](http://nodejs.org/)

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

Now create a new Model based on your schema

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
var alex = new People({name: "Alex", surname: "Rubiks", age: 32});
alex.save(function(err){
    if(!err)
        console.log('Yuppiie!');
});
```

##About

Apollo is brought to you by
- [Niccolò Biondi](https://github.com/bionicco)
- [Elia Cogodi](https://github.com/ecogodi)
- [Fabrizio Rugeri](https://github.com/ramiel)

and with the support of [3logic](http://www.3logic.it)
