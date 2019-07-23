# Local MongoDB Setup

### Install MongoDB Community Edition
Follow MongoDB Manual directions to install MongoDB Community Edition on ubuntu [[3.2](https://docs.mongodb.com/v3.2/tutorial/install-mongodb-on-ubuntu/), [3.4](https://docs.mongodb.com/v3.4/tutorial/install-mongodb-on-ubuntu/), [3.6](https://docs.mongodb.com/v3.6/tutorial/install-mongodb-on-ubuntu/), [4.0](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)] 

### Add users, roles, authentication
Follow [instructions](https://docs.mongodb.com/manual/tutorial/enable-authentication/) to add user administrator and role.

For step 6 (Create additional users as needed for your deployment), use the following role to create a user that can create new dbs and access oplog
```
use test
db.createUser(
  {
    user: "stitch_dev",
    pwd: "<password>",
    roles: [ { role: "readWriteAnyDatabase", db: "admin" }, {role: "read", db: "local"} ]
  }
)
```

### Enable Oplog
1. Edit `/etc/mongod.conf` and add a replciation set:
```
replication:
  replSetName: rs0
```

2. Restart mongod and pass it --config flag:
```
sudo mongod --auth --config /etc/mongod.conf
```

3. Initiate replica set

Connect to shell:
```
mongo --port 27017 -u stitch_dev -p <password> --authenticationDatabase test
```

and initiate replica set:
```
rs.initiate({_id: "replocal", members: [{_id: 0, host: "127.0.0.1:27017"}]})
```

4. Check out that oplog

switch to local
```
use local
```

view oplog rows
```
db.oplog.rs0.find()
```

### Connect with shell
Can now connect to Mongo via the mongo shell with:
```
mongo --host localhost --port 27017 --authenticationDatabase admin --username <username> --password <password>
```

