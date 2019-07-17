import pymongo # requires dnspython package as well


#------------------------ Setup Client ------------------------
#host = 'stitch-upwjw.mongodb.net'
#username = 'harrison'
#password = '2t4FNGc$NrVFF3pRrJc&Vvf2ycpDorMg'
# connection_string = "mongodb+srv://{}:{}@{}/test".format(username, password, host)
# client = pymongo.MongoClient(connection_string)


host=['stitch-shard-00-00-upwjw.mongodb.net',
       'stitch-shard-00-01-upwjw.mongodb.net',
      'stitch-shard-00-02-upwjw.mongodb.net']
username = 'harrison'
password = '2t4FNGc$NrVFF3pRrJc&Vvf2ycpDorMg'
ssl = True # client must have ssl=True to connect to atlas cluster

client = pymongo.MongoClient(host=host, username=username, password=password, port=27017, ssl=True)
test_db = client.test_db

# List dbs
print("\nShowing Initial Databases...")
print(client.list_database_names())


# Make db and collection
# Note: MongoDB waits until you have created a collection (table), with at least one document (record) before it actually creates the database (and collection).
print("\nAdding database=spike_db and collection=sources_team_members...")
spike_db = client["spike_db"]
sources_team_members_coll = spike_db["sources_team_members"]

# Add one document to collection
print ("\nAdding nick to collection=sources_team_members...")
sources_team_members_coll.insert_one({"name": "Nick", "membersince": 2018})

# Add multiple documents to collection
print("\nAdding everyone else to collection=sources_team_members...")
sources_team_members_coll.insert_many([{"name": "Jacob", "membersince": 2019},
                                       {"name": "Collin", "membersince": 2019},
                                       {"name": "Dan", "membersince": 2017},
                                       {"name": "Kyle", "membersince": 2016},
                                       {"name": "Andy", "membersince": 2018},
                                       {"name": "Brian", "membersince": 2014},
                                       {"name": "Harrison", "membersince": 2018}])

# List dbs
print("\nShowing Databases...")
print(client.list_database_names())

# List collections in spike_db
print("\nShowing collections in db=spike_db...")
print(spike_db.list_collection_names())

# Select all  documents from collection
print("\nShowing all documents in sources_team_members_coll...")
for doc in sources_team_members_coll.find():
    print(doc)

# Find documents where membersince> 2016
print("\nShowing documents where membersince > 2016...")
for doc in sources_team_members_coll.find({"membersince": {"$gt": 2016}}):
    print(doc)

# Update Nick's membersince year to 2017
print("\nUpdating Nick's membersince from 2017->2018...")
update_result = sources_team_members_coll.update_one({"name": "Nick"}, {"$set": {"membersince": 2017}})
for doc in sources_team_members_coll.find():
    print(doc)
    
# Update documents in collection
print("\nUpdating to add team field to all documents...")
update_result = sources_team_members_coll.update_many({}, {"$set": {"team": "sources"}})
for doc in sources_team_members_coll.find():
    print(doc)

# Delete document in collection
print("\nRemoving Harrison because he is NOT part of the team...")
delete_result = sources_team_members_coll.delete_many({"name": "Harrison"})
for doc in sources_team_members_coll.find():
    print(doc)

# Delete collection and database
# Removing the collection also removes the database since it is the only collection in the db
print("\nDeleting the collection and database...")
sources_team_members_coll.drop()
print("\nShowing Databases...")
print(client.list_database_names())

