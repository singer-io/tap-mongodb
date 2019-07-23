# Local MongoDB Setup

1. Follow MongoDB Manual directions to install MongoDB Community Edition on ubuntu [[3.2](https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-ubuntu/), [3.4](https://docs.mongodb.com/v3.4/tutorial/install-mongodb-on-ubuntu/), [3.6](https://docs.mongodb.com/v3.6/tutorial/install-mongodb-on-ubuntu/), [4.0](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)] 

2. Follow [instructions](https://docs.mongodb.com/manual/tutorial/enable-authentication/) to add user administrator and role.

For step 6 (Create additional users as needed for your deployment), use the following role to create a user than can create new dbs
```
use admin
db.createUser(
  {
    user: "<new_username>",
    pwd: "<password>",
    roles: [ { role: "readWriteAnyDatabase", db: "admin" } ]
  }
)
```

3. Can now connect to Mongo via the mongo shell with:
```
mongo --host localhost --port 27017 --authenticationDatabase admin --username <username> --password <password>
```

