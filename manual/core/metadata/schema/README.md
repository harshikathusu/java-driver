## Schema metadata

[Metadata#getKeyspaces] returns a client-side representation of the database schema:

```java
Map<CqlIdentifier, KeyspaceMetadata> keyspaces = cluster.getMetadata().getKeyspaces();
KeyspaceMetadata system = keyspaces.get(CqlIdentifier.fromCql("system"));
System.out.println("system.local contains the following tables:");
for (TableMetadata table : system.getTables().values()) {
  System.out.printf(
      "  %s (%d columns)%n", table.getName().asCql(true), table.getColumns().size());
}
```

Schema metadata is fully immutable (both the map and all the objects it contains). It represents a
snapshot of the database at the time of the last metadata refresh, and is consistent with the
[token map](../token/) of its parent `Metadata` object. Keep in mind that `Metadata` is itself
immutable; if you need to get the latest schema, be sure to call
`cluster.getMetadata().getKeyspaces()` again (and not just `getKeyspaces()` on a stale `Metadata`
reference).


### Notifications

If you need to follow schema changes, you don't need to poll the metadata manually; instead,
you can register a listener to get notified when changes occur:

```java
SchemaChangeListener listener =
  new SchemaChangeListenerBase() {
    @Override
    public void onTableCreated(TableMetadata table) {
      System.out.println("New table: " + table.getName().asCql(true));
    }
  };
cluster.register(listener);

session.execute("CREATE TABLE test.foo (k int PRIMARY KEY)");
```

See [SchemaChangeListener] for the list of available methods. [SchemaChangeListenerBase] is a
convenience implementation with empty methods, for when you only need to override a few of them.


### Configuration

#### Enabling/disabling

You can disable schema metadata globally from the configuration:

```
datastax-java-driver.metadata.schema.enabled = false
```

If it is disabled at startup, [Metadata#getKeyspaces] will stay empty. If you disable it at runtime,
it will keep the value of the last refresh.

You can achieve the same thing programmatically with [Cluster#setSchemaMetadataEnabled]: if you call
it with `true` or `false`, it overrides the configuration; if you pass `null`, it reverts to the
value defined in the configuration. One case where that could come in handy is if you are sending a
large number of DDL statements from your code:

```java
// Disable temporarily, we'll do a single refresh once we're done 
cluster.setSchemaMetadataEnabled(false);

for (int i = 0; i < 100; i++) {
  session.execute(String.format("CREATE TABLE test.foo%d (k int PRIMARY KEY)", i));
}

cluster.setSchemaMetadataEnabled(null);
```

Whenever schema metadata was disabled and becomes enabled again (either through the configuration or
the API), a refresh is triggered immediately.


#### Filtering

You can also limit the metadata to a subset of keyspaces: 

```
datastax-java-driver.metadata.schema.refreshed-keyspaces = [ "users", "products" ]
```

If the property is absent or the list is empty, it is interpreted as "all keyspaces".

Note that, if you change the list at runtime, `onKeyspaceAdded`/`onKeyspaceDropped` will be invoked
on your schema listeners for the newly included/excluded keyspaces. 


#### Relation to token metadata

Some of the data in the [token map](../token/) relies on keyspace metadata (any method that takes a
`CqlIdentifier` argument). If schema metadata is disabled or filtered, token metadata will also be
unavailable for the excluded keyspaces.


[Metadata#getKeyspaces]:            http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/Metadata.html#getKeyspaces--
[SchemaChangeListener]:             http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListener.html
[SchemaChangeListenerBase]:         http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/metadata/schema/SchemaChangeListenerBase.html
[Cluster#setSchemaMetadataEnabled]: http://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/Cluster.html#setSchemaMetadataEnabled-java.lang.Boolean-