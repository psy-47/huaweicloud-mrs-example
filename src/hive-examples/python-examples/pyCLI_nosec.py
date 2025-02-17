from pyhs2.haconnection import HAConnection

#"xxx.xxx.xxx.xxx"is ip address
hosts = ["xxx.xxx.xxx.xxx", "xxx.xxx.xxx.xxx"]
try:
    with HAConnection(hosts = hosts,
                       port = 21066,
                       authMechanism = "PLAIN",
                       user='root',
                       password='******') as haConn:
        with haConn.getConnection() as conn:
            with conn.cursor() as cur:
                # Show databases
                print cur.getDatabases()
                
                # Execute query
                cur.execute("show tables")
                
                # Return column info from query
                print cur.getSchema()
                
                # Fetch table results
                for i in cur.fetch():
                    print i
                    
except Exception, e:
    print e

