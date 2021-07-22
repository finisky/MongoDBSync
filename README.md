# MongoDBSync
MongoDB master-slave sync by the change stream

Implementation notes:
* Use transaction to guarantee replay operation success
* NOT use bulkwrite for simplicity
* Make sure your source MongoDB version >= v4.4.4, see also: [# MongoDB Change Stream Makes the Cluster Down](https://finisky.github.io/changestreamcauseserverrestart.en/)

Refer to this post: [MongoDB Master-Slave Replication by Change Stream](https://finisky.github.io/mongodbslavesync.en/)
