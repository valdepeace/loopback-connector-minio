# loopback-connector-minio

## Installation

In your application root directory, enter this command to install the connector:

```sh
npm install loopback-connector-minio --save
```

This installs the module from npm and adds it as a dependency to the application's `package.json` file.

If you create a MongoDB data source using the data source generator as described below, you don't have to do this, since the generator will run `npm install` for you.

## Creating a Minio data source

For LoopBack 4 users, use the LB4 [Command-line interface](https://loopback.io/doc/en/lb4/Command-line-interface.html) to generate a DataSource with MongoDB connector to your LB4 application. Run [`lb4 datasource`](https://loopback.io/doc/en/lb4/DataSource-generator.html), it will prompt for configurations such as host, post, etc. that are required to connect to a other database.

After setting it up, the configuration can be found under `src/datasources/<DataSourceName>.datasource.ts`, which would look like this:

```ts
const config = {
  endPoint: "play.min.io",
  port: 9000,
  useSSL: true,
  accessKey: "Q3AM3UQ867SPQQA43P2F",
  secretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
};
```

### Connection properties

| Property       | Type&nbsp;&nbsp; | Description                                                                                                                        |
| -------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| connector      | string           | Connector name, either `"loopback-connector-minio"` or `"mongodb"`.                                                                |
| endPoint       | string           | URL to object storage service.                                                                                                     |
| port           | Number           | TCP/IP port number. This input is optional. Default value set to 80 for HTTP and 443 for HTTPs.                                    |
| accessKey      | String           | Access key is like user ID that uniquely identifies your account                                                                   |
| secretKey      | String           | Secret key is the password to your account                                                                                         |
| useSSL         | boolean          | Set this value to ‘true’ to enable secure (HTTPS) access                                                                           |
| sessionToken   | string           | Set this value to provide x-amz-security-token (AWS S3 specific). (Optional)                                                       |
| region         | string           | Set this value to override region cache. (Optional)                                                                                |
| transport      | string           | Set this value to pass in a custom transport. (Optional)                                                                           |
| partSize       | number           | Set this value to override default part size of 64MB for multipart uploads. (Optional)                                             |
| pathStyle      | boolean          | Set this value to override default access behavior (path) for non AWS endpoints. Default is true. (Optional)                       |
| transportAgent | string           | Set this value to provide a custom HTTP(s) agent to handle timeouts, TLS handling, and low-level socket configurations. (Optional) |

## Operations

| Bucket                 | Object                  | Presigned           | Bucket policy   | Notification                |
| ---------------------- | ----------------------- | ------------------- | --------------- | --------------------------- |
| makeBucket             | getObject               | presignedUrl        |                 | getBucketNotification       |
| listBuckets            | getPartialObject        | presignedGetObject  |                 | setBucketNotification       |
| bucketExists           | fGetObject              | presignedPutObject  |                 | removeAllBucketNotification |
| removeBucket           | putObject               | presignedPostPolicy | getBucketPolicy |                             |
| listObjects            | fPutObject              |                     | setBucketPolicy |                             |
| listObjectsV2          | copyObject              |                     |                 | listenBucketNotification    |
| listIncompleteUploads  | statObject              |                     |                 |
| getBucketVersioning    | removeObject            |
| setBucketVersioning    | removeObjects           |
| getBucketTagging       | removeIncompleteUpload  |
| setBucketTagging       | putObjectRetention      |
| removeBucketTagging    | getObjectRetention      |
| setBucketLifecycle     | setObjectTagging        |
| getBucketLifecycle     | removeObjectTagging     |
| removeBucketLifecycle  | getObjectTagging        |
| setObjectLockConfig    | getObjectLegalHold      |
| getObjectLockConfig    | setObjectLegalHold      |
| getBucketEncryption    | composeObject           |
| setBucketEncryption    | selectObjectContent     |
| removeBucketEncryption | 
| setBucketReplication    |
| getBucketReplication   | 
| removeBucketReplication |

# Refs

- https://min.io/docs/minio/linux/developers/javascript/minio-javascript.html?ref=docs-redirect
- https://min.io/docs/minio/linux/developers/javascript/API.html#
