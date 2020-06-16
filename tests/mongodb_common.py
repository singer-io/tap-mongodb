def drop_all_collections(client):
    ############# Drop all dbs/collections #############
    for db_name in client.list_database_names():
        if db_name in ['config', 'local', 'system']:
            continue
        for collection_name in client[db_name].list_collection_names():
            if collection_name in ['system.views', 'system.version', 'system.keys', 'system.users']:
                continue
            print("Dropping database: " + db_name + ", collection: " + collection_name)
            client[db_name][collection_name].drop()
