# app-sdk

This is the Shinzo app SDK. It is intended to be used by anyone building an app with Shinzo.

## Functionality

The SDK exposes the following functionality:

### Start an embedded defra instance

If you are building on Shinzo, it is highly recommended that you embed a defra instance into your application. This will make your queries much faster than querying against a standard API by syncing the data onto the client device passively via a pub/sub relationship. 

To start a defra instance, you'll want to import the `defra` package. To start your defra instance:

`myNode, err := defraStartDefraInstance(myConfig, mySchemaApplier)`

You'll also need to construct some inputs:

#### 1. Configuration

The `config` package exposes functionality to read configuration from a yaml file. We have provided an example `config.yaml`. Once you have a config.yaml (or otherwise named), you can call:

```
configPath := "config.yaml"
myConfig, err := config.LoadConfig(configPath)
```

or, if you're not sure about where the config file is (e.g. maybe you're writing a test), you can call:

```
expectedPath := "config.yaml"
configPath, err := file.FindFile(expectedPath)
```

This will do a naive search for the file at some common relative paths, making loading config a bit easier.

You can, of course, also construct or modify a config.Config object by hand.

#### 2. Schema Applier

You will need to provide an implementation of the `SchemaApplier` interface. Currently, we provide implementations of the `SchemaApplier` interface: `SchemaApplierFromFile` and `SchemaApplierFromProvidedSchema`.

When creating a `SchemaApplierFromFile`, you may optionally provide a `DefaultPath` string where your schema resides - this should be a relative path - or you can place your schema in `schema/schema.graphql` and provide no `DefaultPath`.

A `SchemaApplierFromProvidedSchema` should be created with `NewSchemaApplierFromProvidedSchema`, providing it with a schema in string format - helpful for tests or if you've already read your schema from a file.

### Querying your defra instance

Querying your defra instance is made much simpler using the query functions in the defra package.

Query with either `QuerySingle` or `QueryArray` for individual objects or arrays. You'll need to provide a graphql query string and you'll need to define a struct representing the resulting object you hope to receive.

`result, err := defra.QuerySingle[MyResultStruct](myNode, ctx, queryString)`
or
`results, err := defra.QueryArray[MyResultStruct](myNode, ctx, queryString)`

### Writing data to your defra instance

Writing data to your defra instance is made simple using the `PostMutation` function in the defra package.

`result, err := defra.PostMutation[MyResultStruct](ctx, myNode, mutationString)`

This function will execute a GraphQL mutation and return the result unmarshaled into your specified struct type.

For an example on how you can use this query to create complex objects (with relations to other objects), checkout `pkg/defra/complexObjectWriteAndQuery_test.go`.
