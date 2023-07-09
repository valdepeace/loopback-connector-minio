// Copyright IBM Corp. 2012,2020. All Rights Reserved.
// Node module: loopback-connector-mongodb
// This file is licensed under the MIT License.
// License text available at https://opensource.org/licenses/MIT

'use strict';

/*!
 * Module dependencies
 */
const g = require('strong-globalize')();
const minio = require('minio');
const util = require('util');
const Connector = require('loopback-connector').Connector;
const debug = require('debug')('loopback:connector:minio');

/**
 * Initialize the MongoDB connector for the given data source
 * @param {DataSource} dataSource The data source instance
 * @param {Function} [callback] The callback function
 */
exports.initialize = function initializeDataSource(dataSource, callback) {
  if (!minio) {
    return;
  }

  const s = dataSource.settings;
  // https://min.io/docs/minio/linux/developers/javascript/API.html#new-minio-client-endpoint-port-usessl-accesskey-secretkey-region-transport-sessiontoken-partsize

  dataSource.connector = new MinioDB(s, dataSource);
  dataSource.connector.dataSource = dataSource;

  if (callback) {
    if (s.lazyConnect) {
      process.nextTick(function() {
        callback();
      });
    } else {
      dataSource.connector.connect(callback);
    }
  }
};

exports.MinioDB = MinioDB;

/**
 * The constructor for Minio connector
 * @param {Object} settings The settings object
 * @param {DataSource} dataSource The data source instance
 * @constructor
 */
function MinioDB(settings, dataSource) {
  settings = settings || {};
  Connector.call(this, 'miniodb', settings);
  this.settings = settings;
  this.endPoint = settings.endPoint;
  this.port = settings.port;
  this.useSSL = settings.useSSL;
  this.accessKey = settings.accessKey;
  this.secretKey = settings.secretKey;
  this.region = settings.region;
  this.transport = settings.transport;
  this.sessionToken = settings.sessionToken;
  this.partSize = settings.partSize;
  this.pathStyle = settings.pathStyle;
  this.transportAgent = settings.transportAgent;
  this.bucketName = settings.bucketName;
  this.debug = settings.debug || debug.enabled;

  if (this.debug) {
    debug('Settings: %j', settings);
  }

  this._models = {};
  this.DataAccessObject = function() {
    // Dummy function
  };
}

util.inherits(MinioDB, Connector);

/**
 * Connect to MongoDB
 * @param {Function} [callback] The callback function
 *
 * @callback callback
 * @param {Error} err The error object
 * @param {Db} db The mongo DB object
 */
MinioDB.prototype.connect = function(callback) {
  const self = this;
  if (self.client) {
    process.nextTick(function() {
      if (callback) callback(null, self.client);
    });
  } else if (self.dataSource.connecting) {
    self.dataSource.once('connected', function() {
      process.nextTick(function() {
        if (callback) callback(null, self.client);
      });
    });
  } else {
    const validOptionNames = [
      'bucketName',
      'endPoint',
      'port',
      'useSSL',
      'accessKey',
      'secretKey',
      'region',
      'transport',
      'sessionToken',
      'partSize',
      'pathStyle',
      'transportAgent',
    ];

    const lbOptions = Object.keys(self.settings);
    const validOptions = {};
    lbOptions.forEach(function(option) {
      if (validOptionNames.indexOf(option) > -1) {
        validOptions[option] = self.settings[option];
      }
    });
    debug('Valid options: %j', validOptions);

    function onError(err) {
      /* istanbul ignore if */
      if (self.debug) {
        g.error(
          '{{MinioDB}} connection is failed: %s %s',
          self.settings.endpoint,
          err,
        );
      }
      if (callback) callback(err);
    }

    self.connector = self;
    self.client = new minio.Client(validOptions);
    self.setupDataAccessObject();
    return callback(null, self.client);
  }
};
MinioDB.prototype.setupDataAccessObject = function() {
  const self = this;
  // Bucket
  this.DataAccessObject.makeBucket = MinioDB.prototype.makeBucket.bind(self);
  this.DataAccessObject.bucketExists = MinioDB.prototype.bucketExists.bind(self);
  this.DataAccessObject.removeBucket = MinioDB.prototype.removeBucket.bind(self);
  this.DataAccessObject.listBuckets = MinioDB.prototype.listBuckets.bind(self);
  this.DataAccessObject.bucketExists = MinioDB.prototype.bucketExists.bind(self);
  this.DataAccessObject.listObjects = MinioDB.prototype.listObjects.bind(self);
  this.DataAccessObject.listObjectsV2 = MinioDB.prototype.listObjectsV2.bind(self);
  this.DataAccessObject.listIncompleteUploads = MinioDB.prototype.listIncompleteUploads.bind(self);
  this.DataAccessObject.getBucketVersioning = MinioDB.prototype.getBucketVersioning.bind(self);
  this.DataAccessObject.setBucketVersioning = MinioDB.prototype.setBucketVersioning.bind(self);
  this.DataAccessObject.getBucketTagging = MinioDB.prototype.getBucketTagging.bind(self);
  this.DataAccessObject.setBucketTagging = MinioDB.prototype.setBucketTagging.bind(self);
  this.DataAccessObject.removeBucketTagging = MinioDB.prototype.removeBucketTagging.bind(self);
  this.DataAccessObject.setBucketLifecycle = MinioDB.prototype.setBucketLifecycle.bind(self);
  this.DataAccessObject.getBucketLifecycle = MinioDB.prototype.getBucketLifecycle.bind(self);
  this.DataAccessObject.removeBucketLifecycle = MinioDB.prototype.removeBucketLifecycle.bind(self);
  this.DataAccessObject.setBucketEncryption = MinioDB.prototype.setBucketEncryption.bind(self);
  this.DataAccessObject.getBucketEncryption = MinioDB.prototype.getBucketEncryption.bind(self);
  this.DataAccessObject.removeBucketEncryption = MinioDB.prototype.removeBucketEncryption.bind(self);
  this.DataAccessObject.setBucketReplication = MinioDB.prototype.setBucketReplication.bind(self);
  this.DataAccessObject.getBucketReplication = MinioDB.prototype.getBucketReplication.bind(self);
  this.DataAccessObject.removeBucketReplication = MinioDB.prototype.removeBucketReplication.bind(self);
  this.DataAccessObject.setObjectLockConfig = MinioDB.prototype.setObjectLockConfig.bind(self);
  this.DataAccessObject.getObjectLockConfig = MinioDB.prototype.getObjectLockConfig.bind(self);
  // bucket policy
  this.DataAccessObject.setBucketPolicy = MinioDB.prototype.setBucketPolicy.bind(self);
  this.DataAccessObject.getBucketPolicy = MinioDB.prototype.getBucketPolicy.bind(self);
  this.DataAccessObject.setBucketNotification = MinioDB.prototype.setBucketNotification.bind(self);
  this.DataAccessObject.getBucketNotification = MinioDB.prototype.getBucketNotification.bind(self);

  // Objects
  this.DataAccessObject.getObject = MinioDB.prototype.getObject.bind(self);
  this.DataAccessObject.getPartialObject = MinioDB.prototype.getPartialObject.bind(self);
  this.DataAccessObject.fGetObject = MinioDB.prototype.fGetObject.bind(self);
  this.DataAccessObject.putObject = MinioDB.prototype.putObject.bind(self);
  this.DataAccessObject.fPutObject = MinioDB.prototype.fPutObject.bind(self);
  this.DataAccessObject.copyObject = MinioDB.prototype.copyObject.bind(self);
  this.DataAccessObject.statObject = MinioDB.prototype.statObject.bind(self);
  this.DataAccessObject.removeObject = MinioDB.prototype.removeObject.bind(self);
  this.DataAccessObject.removeObjects = MinioDB.prototype.removeObjects.bind(self);
  this.DataAccessObject.removeIncompleteUpload = MinioDB.prototype.removeIncompleteUpload.bind(self);
  this.DataAccessObject.putObjectRetention = MinioDB.prototype.putObjectRetention.bind(self);
  this.DataAccessObject.getObjectRetention = MinioDB.prototype.getObjectRetention.bind(self);

  this.DataAccessObject.getObjectLegalHold = MinioDB.prototype.getObjectLegalHold.bind(self);
  this.DataAccessObject.setObjectTagging = MinioDB.prototype.setObjectTagging.bind(self);
  this.DataAccessObject.getObjectTagging = MinioDB.prototype.getObjectTagging.bind(self);
  this.DataAccessObject.removeObjectTagging = MinioDB.prototype.removeObjectTagging.bind(self);
  this.DataAccessObject.composeObject = MinioDB.prototype.composeObject.bind(self);
  this.DataAccessObject.selectObjectContent = MinioDB.prototype.selectObjectContent.bind(self);
  // Presigned
  this.DataAccessObject.presignedUrl = MinioDB.prototype.presignedUrl.bind(self);
  this.DataAccessObject.presignedGetObject = MinioDB.prototype.presignedGetObject.bind(self);
  this.DataAccessObject.presignedPutObject = MinioDB.prototype.presignedPutObject.bind(self);
  this.DataAccessObject.presignedPostPolicy = MinioDB.prototype.presignedPostPolicy.bind(self);

  this.dataSource.DataAccessObject = this.DataAccessObject;
  for (const model in this._models) {
    if (debug.enabled) {
      debug('Mixing methods into : %s', model);
    }
    this.dataSource.mixin(this._models[model].model);
  }
  return this.DataAccessObject;
};

MinioDB.prototype.getTypes = function() {
  return ['db', 'files', 'miniodb'];
};
/**
 * Creates a new bucket in Minio.
 * @param {string} bucketName - Name of the bucket to create.
 * @param {string} [region] - Region in which to create the bucket. If not provided, the default region is used.
 * @returns {Promise<void>} A Promise that resolves when the bucket is created successfully.
 * @throws {Error} If there is an error creating the bucket.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#makebucket-bucketname-region-makeopts-callback
 * @example
 * minioDB.makeBucket('my-bucket', 'us-east-1')
 *   .then(() => {
 *     // Bucket created successfully
 *   })
 *   .catch((error) => {
 *     // Error creating the bucket
 *   });
 */
MinioDB.prototype.makeBucket = function(bucketName, region) {
  const self = this;
  if (self.debug) {
    debug('makeBucket: %s %s', bucketName, region);
  }

  return new Promise((resolve, reject) => {
    if (region === undefined) {
      self.client.makeBucket(bucketName, function(err) {
        if (err) {
          if (self.debug) {
            g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
          }
          reject(err);
        } else {
          resolve();
        }
      });
    } else {
      self.client.makeBucket(bucketName, region, function(err) {
        if (err) {
          if (self.debug) {
            g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
          }
          reject(err);
        } else {
          resolve();
        }
      });
    }
  });
};
/**
 * Retrieves a list of buckets in Minio.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#listbuckets-callback
 * @returns {Promise<Array<Object>>} A Promise that resolves with an array of bucket objects.
 * Each bucket object has the following properties:
 *   - name: The name of the bucket.
 *   - creationDate: The date when the bucket was created.
 * @throws {Error} If there is an error retrieving the list of buckets.
 * @example
 * minioDB.listBuckets()
 *   .then((buckets) => {
 *     // Successfully retrieved list of buckets
 *     buckets.forEach((bucket) => {
 *       console.log(bucket.name, bucket.creationDate);
 *     });
 *   })
 *   .catch((error) => {
 *     // Error retrieving list of buckets
 *   });
 */
MinioDB.prototype.listBuckets = function() {
  const self = this;
  if (self.debug) {
    debug('listBuckets');
  }

  return new Promise((resolve, reject) => {
    self.client.listBuckets(function(err, buckets) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} listBuckets is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(buckets);
      }
    });
  });
};
/**
 * Checks if a bucket exists in Minio.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#bucketexists-bucketname-callback
 * @param {string} bucketName - The name of the bucket to check.
 * @returns {Promise<boolean>} A Promise that resolves with a boolean value indicating
 * whether the bucket exists (true) or not (false).
 * @throws {Error} If there is an error checking the bucket existence.
 * @example
 * minioDB.bucketExists('my-bucket')
 *   .then((exists) => {
 *     if (exists) {
 *       console.log('Bucket exists');
 *     } else {
 *       console.log('Bucket does not exist');
 *     }
 *   })
 *   .catch((error) => {
 *     // Error checking bucket existence
 *   });
 */
MinioDB.prototype.bucketExists = async function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('bucketExists: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.bucketExists(bucketName, (err, exists) => {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(exists);
      }
    });
  });
};

/**
 * Remove a bucket from Minio.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removebucket-bucketname-callback
 * @param {string} bucketName - Name of the bucket to remove.
 * @returns {Promise} A promise that resolves with no value when the bucket is successfully removed.
 *                    It rejects with an error if any problem occurs.
 */
MinioDB.prototype.removeBucket = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('removeBucket: %s', bucketName);
  }

  return new Promise((resolve, reject) => {
    self.client.removeBucket(bucketName, function(err) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

/**
 * List objects in a Minio bucket.
 * https://min.io/docs/minio/linux/developers/javascript/API.html#listobjects-bucketname-prefix-recursive-listopts
 * @param {string} bucketName - Name of the bucket.
 * @param {string} prefix - Prefix to filter the objects (optional).
 * @param {boolean} recursive - Indicates if the object listing should be recursive (optional).
 * @param {object} listOpts - Additional options to pass to the list objects operation (optional).
 * @returns {Promise} A promise that resolves with the list of objects when the operation is successfully completed.
 *                    It rejects with an error if any problem occurs.
 */
MinioDB.prototype.listObjects = function(bucketName, prefix, recursive, listOpts) {
  const self = this;
  if (self.debug) {
    debug('listObjects: %s %s %s %s', bucketName, prefix, recursive, listOpts);
  }

  return new Promise((resolve, reject) => {
    const stream = self.client.listObjects(bucketName, prefix, recursive, listOpts);
    resolve(stream);
    // self.client.listObjects(bucketName, prefix, recursive, function(err, data) {
    //   if (err) {
    //     if (self.debug) {
    //       g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
    //     }
    //     reject(err);
    //   } else {
    //     resolve(data);
    //   }
    // });
  });
};

/**
 * List objects in a Minio bucket using the version 2 of the method.
 * https://min.io/docs/minio/linux/developers/javascript/API.html#listobjectsv2-bucketname-prefix-recursive-startafter
 * @param {string} bucketName - Name of the bucket.
 * @param {string} prefix - Prefix to filter the objects (optional).
 * @param {boolean} recursive - Indicates if the object listing should be recursive (optional).
 * @returns {Promise} A promise that resolves with the list of objects when the operation is successfully completed.
 *                    It rejects with an error if any problem occurs.
 */
MinioDB.prototype.listObjectsV2 = function(bucketName, prefix, recursive) {
  const self = this;
  if (self.debug) {
    debug('listObjectsV2: %s %s %s', bucketName, prefix, recursive);
  }

  return new Promise((resolve, reject) => {
    const stream = self.client.listObjectsV2({Bucket: bucketName, Prefix: prefix, Recursive: recursive});
    resolve(stream);
    // self.client.listObjectsV2({Bucket: bucketName, Prefix: prefix, Recursive: recursive}, function(err, data) {
    //   if (err) {
    //     if (self.debug) {
    //       g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
    //     }
    //     reject(err);
    //   } else {
    //     resolve(data);
    //   }
    // });
  });
};
/**
 * Lists objects in a bucket with their metadata using the ListObjectsV2 API of Minio.
 * @param {string} bucketName - The name of the bucket.
 * @param {string} [prefix] - The prefix to filter objects by their names (optional).
 * @param {boolean} [recursive] - Indicates whether to list objects recursively or not (optional, default: false).
 * @returns {Promise<Array<ObjectMetadata>>} A Promise that resolves with an array of object metadata.
 * Each object metadata contains information about an object, such as its name, size, and last modified date.
 * @throws {Error} If there is an error listing the objects.
 * @example
 * minioDB.listObjectsV2WithMetadata('my-bucket')
 *   .then((objects) => {
 *     objects.forEach((object) => {
 *       console.log(`Name: ${object.name}`);
 *       console.log(`Size: ${object.size}`);
 *       console.log(`Last Modified: ${object.lastModified}`);
 *       console.log('---');
 *     });
 *   })
 *   .catch((error) => {
 *     // Error listing objects
 *   });
 */
MinioDB.prototype.listObjectsV2WithMetadata = function(bucketName, prefix, recursive, startAfter) {
  const self = this;
  if (self.debug) {
    debug('listObjectsV2WithMetadata: %s', bucketName);
  }

  return new Promise((resolve, reject) => {
    const objects = [];
    const stream = self.client.listObjectsV2(bucketName, prefix, recursive, startAfter);
    resolve(stream);
    // stream.on('data', (obj) => {
    //   objects.push(obj);
    // });

    // stream.on('error', (err) => {
    //   if (self.debug) {
    //     g.error('{{MinioDB}} listObjectsV2WithMetadata is failed: %s %s', self.settings.endpoint, err);
    //   }
    //   reject(err);
    // });

    // stream.on('end', () => {
    //   resolve(objects);
    // });
  });
};

/**
 * Lists incomplete uploads in a Minio bucket.
 * https://min.io/docs/minio/linux/developers/javascript/API.html#listincompleteuploads-bucketname-prefix-recursive-listopts
 * @param {string} bucketName - Name of the bucket.
 * @param {string} prefix - Prefix to filter the uploads (optional).
 * @param {boolean} recursive - Indicates if the upload listing should be recursive (optional).
 * @param {function} callback - Callback function that handles the result or error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#listincompleteuploads-bucketname-prefix-recursive
 */
MinioDB.prototype.listIncompleteUploads = function(bucketName, prefix, recursive, callback) {
  const self = this;
  if (self.debug) {
    debug('listIncompleteUploads: %s %s %s', bucketName, prefix, recursive);
  }
  return new Promise((resolve, reject) => {
    const stream = self.client.listIncompleteUploads({Bucket: bucketName, Prefix: prefix, Recursive: recursive});
    resolve(stream);
    // self.client.listIncompleteUploads({Bucket: bucketName, Prefix: prefix, Recursive: recursive}, function(err, data) {
    //   if (err) {
    //     if (self.debug) {
    //       g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
    //     }
    //     callback(err);
    //   } else {
    //     callback(null, data);
    //   }
    // });
  });
};

/**
 * Retrieves the versioning status of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {function} callback - Callback function that handles the result or error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbucketversioning-bucketname
 */
MinioDB.prototype.getBucketVersioning = function(bucketName, callback) {
  const self = this;
  if (self.debug) {
    debug('getBucketVersioning: %s', bucketName);
  }
  callback = callback || function() { };
  self.client.getBucketVersioning({Bucket: bucketName}, function(err, data) {
    if (err) {
      if (self.debug) {
        g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
      }
      callback(err);
    } else {
      callback(null, data);
    }
  });
};
/**
 * Sets the versioning status of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} status - Versioning status ("Enabled" or "Suspended").
 * @param {function} callback - Callback function that handles the result or error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setBucketVersioning
 */
MinioDB.prototype.setBucketVersioning = function(bucketName, versioningConfig) {
  const self = this;
  if (self.debug) {
    debug('setBucketVersioning: %s %s', bucketName, versioningConfig);
  }
  new Promise((resolve, reject) => {
    self.client.setBucketVersioning(bucketName, versioningConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};
/**
 * Sets the replication configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object} replicationConfig - Replication configuration object.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketreplication-bucketname-replicationconfig-callback
 */
MinioDB.prototype.setBucketReplication = function(bucketName, replicationConfig) {
  const self = this;
  if (self.debug) {
    debug('setBucketReplication: %s, %s', bucketName, replicationConfig);
  }
  return new Promise((resolve, reject) => {
    self.client.setBucketReplication(bucketName, replicationConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};
/**
 * Retrieves the replication configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the replication configuration or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbucketreplication-bucketname-callback
 */
MinioDB.prototype.getBucketReplication = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getBucketReplication: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketReplication(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes the replication configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removebucketreplication-bucketname-callback
 */
MinioDB.prototype.removeBucketReplication = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('removeBucketReplication: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.removeBucketReplication(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the tagging configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {function} callback - Callback function that handles the result or error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbuckettagging-bucketname-callback
 */
MinioDB.prototype.getBucketTagging = function(bucketName, callback) {
  const self = this;
  if (self.debug) {
    debug('getBucketTagging: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketTagging(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};
/**
* Sets the tagging configuration of a Minio bucket.
* @param {string} bucketName - Name of the bucket.
* @param {object[]} tagging - Array of tags in the format [{ Key: 'tagKey', Value: 'tagValue' }].
* @param {function} callback - Callback function that handles the result or error.
* @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbuckettagging-bucketname-tags-callback
*/
MinioDB.prototype.setBucketTagging = function(bucketName, tags, callback) {
  const self = this;
  if (self.debug) {
    debug('setBucketTagging: %s, %s', bucketName, tags);
  }
  return new Promise((resolve, reject) => {
    self.client.setBucketTagging(bucketName, tags, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes the tagging configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {function} callback - Callback function that handles the result or error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removebuckettagging-bucketname-callback
 */
MinioDB.prototype.removeBucketTagging = function(bucketName, callback) {
  const self = this;
  if (self.debug) {
    debug('removeBucketTagging: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.removeBucketTagging(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the lifecycle configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object[]} rules - Array of lifecycle rules.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketlifecycle-bucketname-lifecycleconfig-callback
 * @example
 * const lifecycleConfig= {
 *     Rule: [{
 *         "ID": "Transition and Expiration Rule",
 *         "Status": "Enabled",
 *         "Filter": {
 *             "Prefix":"",
 *         },
 *         "Expiration": {
 *             "Days": "3650"
 *         }
 *     }
 *    ]
 * }
 *
 * minioClient.setBucketLifecycle('bucketname',lifecycleConfig, function (err) {
 *   if (err) {
 *     return console.log(err)
 *   }
 *   console.log("Success")
 * })
 */
MinioDB.prototype.setBucketLifecycle = function(bucketName, lifecycleConfig) {
  const self = this;
  if (self.debug) {
    debug('setBucketLifecycle: %s', bucketName, lifecycleConfig);
  }
  return new Promise((resolve, reject) => {
    self.client.putBucketLifecycleConfiguration(bucketName, lifecycleConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the lifecycle configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbuckettagging-bucketname-callback
 */
MinioDB.prototype.getBucketLifecycle = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getBucketLifecycle: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketLifecycleConfiguration(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes the lifecycle configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removebuckettagging-bucketname-callback
 */
MinioDB.prototype.removeBucketLifecycle = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('removeBucketLifecycle: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.removeBucketLifecycleConfiguration(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the object lock configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} mode - Object lock mode ("enabled" or "disabled").
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setobjectlockconfig-bucketname-lockconfig-callback
 */
MinioDB.prototype.setObjectLockConfig = function(bucketName, lockConfig) {
  const self = this;
  if (self.debug) {
    debug('setObjectLockConfig: %s %s', bucketName, lockConfig);
  }
  return new Promise((resolve, reject) => {
    self.client.putObjectLockConfiguration(bucketName, lockConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the object lock configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getobjectlockconfig-bucketname-callback
 */
MinioDB.prototype.getObjectLockConfig = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getObjectLockConfig: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getObjectLockConfiguration(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the encryption configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketencryption-bucketname-encryptionconfig-callback
 */
MinioDB.prototype.getBucketEncryption = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getBucketEncryption: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketEncryption(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the encryption configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object[]} rules - Array of encryption rules.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketencryption-bucketname-encryptionconfig-callback
 */
MinioDB.prototype.setBucketEncryption = function(bucketName, encryptionConfig) {
  const self = this;
  if (self.debug) {
    debug('setBucketEncryption: %s, %s', bucketName, encryptionConfig);
  }
  return new Promise((resolve, reject) => {
    self.client.setBucketEncryption(bucketName, encryptionConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes the encryption configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removebucketencryption-bucketname-callback
 */
MinioDB.prototype.removeBucketEncryption = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('removeBucketEncryption: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.removeBucketEncryption(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the object lock configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbucketencryption-bucketname-callback
 */
MinioDB.prototype.getObjectLockConfig = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getObjectLockConfig: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getObjectLockConfiguration(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/// Object operations ///

/**
 * Retrieves an object from a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @returns {Promise<Buffer>} A Promise that resolves with the object data as a Buffer.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getobject-bucketname-objectname-getopts-callback
 */
MinioDB.prototype.getObject = function(bucketName, objectName, getOpts) {
  const self = this;
  if (self.debug) {
    debug('getObject: %s %s %s', bucketName, objectName, getOpts);
  }

  return new Promise((resolve, reject) => {
    const stream = self.client.getObject(bucketName, objectName, getOpts);
    resolve(stream);
    // self.client.getObject(params, function(err, dataStream) {
    //   if (err) {
    //     if (self.debug) {
    //       g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
    //     }
    //     reject(err);
    //   } else {
    //     const chunks = [];
    //     dataStream.on('data', function(chunk) {
    //       chunks.push(chunk);
    //     });
    //     dataStream.on('end', function() {
    //       const buffer = Buffer.concat(chunks);
    //       resolve(buffer);
    //     });
    //     dataStream.on('error', function(err) {
    //       reject(err);
    //     });
    //   }
    // });
  });
};
/**
 * Retrieves a partial object from a Minio bucket(reads 30 bytes from the offset 10.).
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {number} offset - Offset in bytes.
 * @param {number} length - Length in bytes.
 * @param {object} getOpts - Optional parameters.
 * @returns {Promise} A Promise that resolves with the buffer or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getpartialobject-bucketname-objectname-offset-length-getopts-callback
 */
MinioDB.prototype.getPartialObject = function(bucketName, objectName, offset, length, getOpts) {
  const self = this;
  if (self.debug) {
    debug('getPartialObject: %s %s %d %d %s', bucketName, objectName, offset, length, getOpts);
  }
  return new Promise((resolve, reject) => {
    const chunks = [];
    // reads 30 bytes from the offset 10.
    const dataStream = self.client.getPartialObject(bucketName, objectName, offset, length, getOpts);
    resolve(dataStream);
    // dataStream.on('data', function(chunk) {
    //   chunks.push(chunk);
    // });
    // dataStream.on('end', function() {
    //   const buffer = Buffer.concat(chunks);
    //   resolve(buffer);
    // });
    // dataStream.on('error', function(err) {
    //   if (self.debug) {
    //     g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
    //   }
    //   reject(err);
    // });
  });
};
/**
 * Retrieves an object from a Minio bucket and saves it to a file.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {string} filePath - Path to the file.
 * @returns {Promise} A Promise that resolves with the stat or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#fgetobject-bucketname-objectname-filepath-getopts-callback
 */
MinioDB.prototype.fGetObject = function(bucketName, objectName, filePath) {
  const self = this;
  if (self.debug) {
    debug('fGetObject: %s %s %s', bucketName, objectName, filePath);
  }
  return new Promise((resolve, reject) => {
    self.client.fGetObject(bucketName, objectName, filePath, function(err, stat) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
};

/**
 * Uploads an object to a Minio bucket from a readable stream.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {ReadableStream} stream - Readable stream to upload.
 * @param {number} size - Size of the object in bytes.
 * @param {object} putOpts - Options for the putObject operation.
 * @returns {Promise} A Promise that resolves with the etag or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#putobject-bucketname-objectname-stream-size-metadata-callback
 * @example
 * var Fs = require('fs')
 * var file = '/tmp/40mbfile'
 * var fileStream = Fs.createReadStream(file)
 * var fileStat = Fs.stat(file, function(err, stats) {
 *   if (err) {
 *     return console.log(err)
 *   }
 *   minioClient.putObject('mybucket', '40mbfile', fileStream, stats.size, function(err, objInfo) {
 *       if(err) {
 *           return console.log(err) // err should be null
 *       }
 *    console.log("Success", objInfo)
 *   })
 * })
 */
MinioDB.prototype.putObject = function(bucketName, objectName, stream, size, putOpts) {
  const self = this;
  if (self.debug) {
    debug('putObject: %s %s %j', bucketName, objectName, putOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.putObject(bucketName, objectName, stream, size, putOpts, function(err, etag) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(etag);
      }
    });
  });
};

/**
 * Uploads an object to a Minio bucket from a local file.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {string} filePath - Path to the file.
 * @returns {Promise} A Promise that resolves with the stat or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#fputobject-bucketname-objectname-filepath-metadata-callback
 * @example
 * var file = '/tmp/40mbfile'
 * var metaData = {
 *   'Content-Type': 'text/html',
 *   'Content-Language': 123,
 *   'X-Amz-Meta-Testing': 1234,
 *   'example': 5678
 * }
 * minioClient.fPutObject('mybucket', '40mbfile', file, metaData, function(err, objInfo) {
 *     if(err) {
 *         return console.log(err)
 *     }
 *     console.log("Success", objInfo.etag, objInfo.versionId)
 * })
 */
MinioDB.prototype.fPutObject = function(bucketName, objectName, filePath) {
  const self = this;
  if (self.debug) {
    debug('fPutObject: %s %s %s', bucketName, objectName, filePath);
  }
  return new Promise((resolve, reject) => {
    self.client.fPutObject(bucketName, objectName, filePath, function(err, stat) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
};

/**
 * Copies an object from one Minio bucket to another.
 * @param {string} bucketName - Name of the source bucket.
 * @param {string} objectName - Name of the source object.
 * @param {string} destBucket - Name of the destination bucket.
 * @param {string} destObjectName - Name of the destination object.
 * @returns {Promise} A Promise that resolves with the stat or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#copyobject-bucketname-objectname-sourceobject-conditions-callback
 */
MinioDB.prototype.copyObject = function(bucketName, objectName, destBucket, destObjectName) {
  const self = this;
  if (self.debug) {
    debug('copyObject: %s %s %s %s', bucketName, objectName, destBucket, destObjectName);
  }
  return new Promise((resolve, reject) => {
    self.client.copyObject(bucketName, objectName, destBucket, destObjectName, function(err, stat) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} copyObject is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
};

/**
 * Retrieves metadata of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @returns {Promise} A Promise that resolves with the stat or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#statobject-bucketname-objectname-statopts-callback
 */
MinioDB.prototype.statObject = function(bucketName, objectName) {
  const self = this;
  if (self.debug) {
    debug('statObject: %s %s', bucketName, objectName);
  }
  return new Promise((resolve, reject) => {
    self.client.statObject(bucketName, objectName, function(err, stat) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
};

/**
 * Removes an object from a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} removeOpts - Optional parameters. Version of the object in the form {versionId:"my-versionId", governanceBypass: true or false }. Default is {}. (Optional)
 * @returns {Promise} A Promise that resolves with no value or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removeobjects-bucketname-objectslist-callback
 */
MinioDB.prototype.removeObject = function(bucketName, objectName, removeOpts) {
  const self = this;
  if (self.debug) {
    debug('removeObject: %s %s', bucketName, objectName, removeOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.removeObject(bucketName, objectName, removeOpts, function(err) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(true);
      }
    });
  });
};

/**
 * Removes multiple objects from a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object[]} objects - Array of objects to remove.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removeobjects-bucketname-objectslist-callback
 */
MinioDB.prototype.removeObjects = function(bucketName, objectList) {
  const self = this;
  if (self.debug) {
    debug('removeObjects: %s', bucketName, objectList);
  }
  return new Promise((resolve, reject) => {
    self.client.removeObjects(bucketName, objectList, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes an incomplete upload of an object from a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removeincompleteupload-bucketname-objectname-callback
 */
MinioDB.prototype.removeIncompleteUpload = function(bucketName, objectName) {
  const self = this;
  if (self.debug) {
    debug('removeIncompleteUpload: %s %s', bucketName, objectName);
  }
  return new Promise((resolve, reject) => {
    self.client.removeIncompleteUpload(bucketName, objectName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the retention configuration of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} retentionOpts - Retention configuration of the object in the form {mode:"mode", retainUntilDate:"date"}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#putobjectretention-bucketname-objectname-retentionopts-callback
 */
MinioDB.prototype.putObjectRetention = function(bucketName, objectName, retentionOpts) {
  const self = this;
  if (self.debug) {
    debug('putObjectRetention: %s %s %s %s', bucketName, objectName, retentionOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.putObjectRetention(bucketName, objectName, retentionOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the retention configuration of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} getOpts - Get options. Options for retention like : { versionId:"my-versionId" } Default is {} (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getobjectretention-bucketname-objectname-getopts-callback
 */
MinioDB.prototype.getObjectRetention = function(bucketName, objectName, getOpts) {
  const self = this;
  if (self.debug) {
    debug('getObjectRetention: %s %s', bucketName, objectName, getOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.getObjectRetention(bucketName, objectName, getOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the retention configuration of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {string} mode - Retention mode.
 * @param {Date|string} retainUntilDate - Retain until date.
 * @returns {Promise} A Promise that resolves with no value or rejects with an error.
 * @link https://docs.min.io/docs/javascript-client-api-reference#setObjectRetention
 */
MinioDB.prototype.setObjectRetention = function(bucketName, objectName, mode, retainUntilDate) {
  const self = this;
  if (self.debug) {
    debug('setObjectRetention: %s %s %s %s', bucketName, objectName, mode, retainUntilDate);
  }
  return new Promise((resolve, reject) => {
    self.client.setObjectRetention(bucketName, objectName, mode, retainUntilDate, function(err) {
      if (err) {
        if (self.debug) {
          g.error(
            '{{MinioDB}} setObjectRetention is failed: %s %s',
            self.settings.endpoint,
            err,
          );
        }
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

/**
 * Sets the tags of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object[]} tags - Array of tags.
 * @param {object} putOpts - Default is {}. e.g {versionId:"my-version-id"}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://docs.min.io/docs/javascript-client-api-reference#putObjectTagging
 */
MinioDB.prototype.setObjectTagging = function(bucketName, objectName, tags, putOpts) {
  const self = this;
  if (self.debug) {
    debug('setObjectTagging: %s %s', bucketName, objectName, tags, putOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.putObjectTagging(bucketName, objectName, tags, putOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Removes the tags of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} removeopts - Default is {}. e.g {versionId:"my-version-id"}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#removeobjecttagging-bucketname-objectname-removeopts-callback
 */
MinioDB.prototype.removeObjectTagging = function(bucketName, objectName, removeopts) {
  const self = this;
  if (self.debug) {
    debug('removeObjectTagging: %s %s', bucketName, objectName, removeopts);
  }
  return new Promise((resolve, reject) => {
    self.client.removeObjectTagging(bucketName, objectName, removeopts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the tags of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} getOpts - Default is {}. e.g {versionId:"my-version-id"}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getobjecttagging-bucketname-objectname-getopts-callback
 */
MinioDB.prototype.getObjectTagging = function(bucketName, objectName, getOpts) {
  const self = this;
  if (self.debug) {
    debug('getObjectTagging: %s %s', bucketName, objectName, getOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.getObjectTagging(bucketName, objectName, getOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Retrieves the legal hold status of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} getOpts - Default is {}. e.g {versionId:"my-version-id"}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getobjectlegalhold-bucketname-objectname-getopts-callback
 */
MinioDB.prototype.getObjectLegalHold = function(bucketName, objectName, getOpts) {
  const self = this;
  if (self.debug) {
    debug('getObjectLegalHold: %s %s', bucketName, objectName, getOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.getObjectLegalHold(bucketName, objectName, getOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the legal hold status of an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {boolean} legalHoldStatus - Legal hold status (true for ON, false for OFF).
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setobjectlegalhold-bucketname-objectname-setopts-callback
 */
MinioDB.prototype.setObjectLegalHold = function(bucketName, objectName, setOpts) {
  const self = this;
  if (self.debug) {
    debug('setObjectLegalHold: %s %s %s', bucketName, objectName, setOpts);
  }
  return new Promise((resolve, reject) => {
    self.client.setObjectLegalHold(bucketName, objectName, setOpts, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Composes multiple objects into a single object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
  * @param {object} sourceObjectList - List of source objects that are to be composed. e.g [{name:"my-objectname",versionId:"my-version-id"},{name:"my-objectname2",versionId:"my-version-id2"}]
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#composeobject-destobjconfig-sourceobjectlist-callback
 */
MinioDB.prototype.composeObject = function(bucketName, sourceObjectList) {
  const self = this;
  if (self.debug) {
    debug('composeObject: %s %s', bucketName, sourceObjectList);
  }
  return new Promise((resolve, reject) => {
    self.client.composeObject(bucketName, sourceObjectList, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Executes an SQL SELECT expression on an object in a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {object} selectOpts - Default is {}. e.g {expression:"select * from S3Object",inputSerialization:{JSON:{Type:"Lines"}},outputSerialization:{JSON:{RecordDelimiter:"\n"}}}. (Optional)
 * @returns {Promise} A Promise that resolves with the result or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#selectobjectcontent-bucketname-objectname-selectopts-callback
 * @example
 * const selectOpts = {
 *     expression:"SELECT * FROM s3object s where s.\"Name\" = 'Jane'",
 *     expressionType:"SQL",
 *     inputSerialization : {'CSV': {"FileHeaderInfo": "Use",
 *             RecordDelimiter: "\n",
 *             FieldDelimiter:  ",",
 *         },
 *         'CompressionType': 'NONE'},
 *     outputSerialization : {'CSV': {RecordDelimiter: "\n",
 *             FieldDelimiter:  ",",}},
 *     requestProgress:{Enabled:true},
 * }
 *
 * minioClient.selectObjectContent('bucketName', 'objectName', selectOpts, function(err, res) {
 *   if (err) {
 *     return console.log('Unable to process select object content.', err.message)
 *   }
 *   console.log('Success')
 * })
 */
MinioDB.prototype.selectObjectContent = function(bucketName, objectName, selectOpts) {
  const self = this;
  if (self.debug) {
    debug('selectObjectContent: %s %s', bucketName, objectName, selectOpts);
  }
  return new Promise((resolve, reject) => {
    const eventStream = self.client.selectObjectContent(bucketName, objectName, selectOpts);
    resolve(eventStream);
    // const chunks = [];
    // eventStream.on('data', function(event) {
    //   if (event.Records) {
    //     chunks.push(event.Records.Payload.toString());
    //   }
    // });
    // eventStream.on('error', function(err) {
    //   if (self.debug) {
    //     g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
    //   }
    //   reject(err);
    // });
    // eventStream.on('end', function() {
    //   const result = chunks.join('');
    //   resolve(result);
    // });
  });
};

// Presigned Operations

/**
 * Generates a presigned URL for the specified method and object in a Minio bucket.
 * @param {string} method - HTTP method for the presigned URL.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {number} expiry - Expiry time for the presigned URL in seconds.
 * @param {object} reqParams - Additional request parameters.
 * @returns {Promise} A Promise that resolves with the presigned URL or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#presignedurl-httpmethod-bucketname-objectname-expiry-reqparams-requestdate-cb
 */
MinioDB.prototype.presignedUrl = function(method, bucketName, objectName, expiry, reqParams) {
  const self = this;
  if (self.debug) {
    debug('presignedUrl: %s %s %s %s %j', method, bucketName, objectName, expiry, reqParams);
  }
  return new Promise((resolve, reject) => {
    self.client.presignedUrl(method, bucketName, objectName, expiry, reqParams, function(err, presignedUrl) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(presignedUrl);
      }
    });
  });
};

/**
 * Generates a presigned URL for downloading an object from a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {number} expiry - Expiry time for the presigned URL in seconds.
 * @param {object} respHeaders - Response headers that must be present in the response.
 * @param {object} requestDate - A date object.
 * @returns {Promise} A Promise that resolves with the presigned URL or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#presignedgetobject-bucketname-objectname-expiry-respheaders-requestdate-cb
 */
MinioDB.prototype.presignedGetObject = function(bucketName, objectName, expiry, respHeaders, requestDate) {
  const self = this;
  if (self.debug) {
    debug('presignedGetObject: %s %s %s', bucketName, objectName, expiry, respHeaders, requestDate);
  }
  return new Promise((resolve, reject) => {
    self.client.presignedGetObject(bucketName, objectName, expiry, respHeaders, requestDate,
      function(err, presignedUrl) {
        if (err) {
          if (self.debug) {
            g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
          }
          reject(err);
        } else {
          resolve(presignedUrl);
        }
      });
  });
};

/**
 * Generates a presigned URL for uploading an object to a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {number} expiry - Expiry time for the presigned URL in seconds.
 * @returns {Promise} A Promise that resolves with the presigned URL or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#presignedputobject-bucketname-objectname-expiry-callback
 */
MinioDB.prototype.presignedPutObject = function(bucketName, objectName, expiry) {
  const self = this;
  if (self.debug) {
    debug('presignedPutObject: %s %s %s', bucketName, objectName, expiry);
  }
  return new Promise((resolve, reject) => {
    self.client.presignedPutObject(bucketName, objectName, expiry, function(err, presignedUrl) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(presignedUrl);
      }
    });
  });
};

/**
 * Generates a presigned POST policy for uploading an object to a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {string} objectName - Name of the object.
 * @param {string} objectNamePrefix - Prefix of the object name.
 * @param {number} expiresInSeconds - Expiry time for the presigned policy in seconds.
 * @returns {Promise} A Promise that resolves with the presigned POST policy or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#presignedpostpolicy-policy-callback
 */
MinioDB.prototype.presignedPostPolicy = function(bucketName, objectName, objectNamePrefix, expiresInSeconds) {
  const self = this;
  if (self.debug) {
    debug('presignedPostPolicy: %s %s', bucketName, objectName);
  }
  const policy = new self.client.Policy();
  policy.setExpires(new Date(Date.now() + expiresInSeconds * 1000));
  policy.setBucketName(bucketName);
  policy.setKey(objectName);
  policy.setKeyStartsWith(objectNamePrefix);
  return new Promise((resolve, reject) => {
    const policy = {
      expiration: new Date(Date.now() + expiresInSeconds * 1000),
      conditions: [
        ['starts-with', '$key', objectName],
        {bucket: bucketName},
        {acl: 'private'},
      ],
    };
    const formData = self.client.presignedPostPolicy(policy, function(err, formData) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection is failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(formData);
      }
    });
  });
};
// 5 Bucket Policy & Notification Operations

/**
 * Retrieves the notification configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the notification configuration or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbucketnotification-bucketname-cb
 */
MinioDB.prototype.getBucketNotification = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getBucketNotification: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketNotification(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the notification configuration of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object} notificationConfig - Notification configuration object.
 * @returns {Promise} A Promise that resolves when the notification configuration is set or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketnotification-bucketname-bucketnotificationconfig-callback
 */
MinioDB.prototype.setBucketNotification = function(bucketName, notificationConfig) {
  const self = this;
  if (self.debug) {
    debug('setBucketNotification: %s %s', bucketName, notificationConfig);
  }
  return new Promise((resolve, reject) => {
    self.client.setBucketNotification(bucketName, notificationConfig, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};
/**
 * Removes all notification configurations from a bucket.
 * @param {string} bucketName - The name of the bucket.
 * @returns {Promise} A Promise that resolves if the operation is successful, and rejects if there is an error.
 * @throws {Error} If there is an error removing the bucket notification configurations.
 * @example
 * minioDB.removeAllBucketNotification('my-bucket')
 *   .then(() => {
 *     console.log('Bucket notification configurations removed successfully');
 *   })
 *   .catch((error) => {
 *     // Error removing bucket notification configurations
 *   });
 */
MinioDB.prototype.removeAllBucketNotification = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('removeAllBucketNotification: %s', bucketName);
  }

  return new Promise((resolve, reject) => {
    const config = {
      QueueConfiguration: {},
      TopicConfiguration: {},
      LambdaFunctionConfiguration: {},
    };

    self.client.setBucketNotification(bucketName, config, (err) => {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

/**
 * Listens for bucket notifications and invokes the callback function when a notification is received.
 * @param {string} bucketName - The name of the bucket to listen for notifications.
 * @param {function} callback - The callback function to be invoked when a notification is received. It takes two parameters: `err` (the error, if any) and `record` (the notification record).
 * @returns {EventEmitter} An EventEmitter object that can be used to control the subscription and listen for events.
 * @throws {Error} If there is an error setting up the bucket notification listener.
 * @example
 * const listener = minioDB.listenBucketNotification('my-bucket', (err, record) => {
 *   if (err) {
 *     // Error receiving bucket notification
 *   } else {
 *     console.log('Received bucket notification:', record);
 *   }
 * });
 *
 * // To stop listening for notifications
 * listener.removeAllListeners();
 */
MinioDB.prototype.listenBucketNotification = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('listenBucketNotification: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    const listener = self.client.listenBucketNotification(bucketName);
    resolve(listener);
  });
};

/**
 * Get the bucket policy associated with the specified bucket. If objectPrefix is not empty, the bucket policy will be filtered based on object permissions as well.
 * @param {string} bucketName - Name of the bucket.
 * @returns {Promise} A Promise that resolves with the bucket policy (parsed JSON) or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#getbucketpolicy-bucketname-callback
 */
MinioDB.prototype.getBucketPolicy = function(bucketName) {
  const self = this;
  if (self.debug) {
    debug('getBucketPolicy: %s', bucketName);
  }
  return new Promise((resolve, reject) => {
    self.client.getBucketPolicy(bucketName, function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

/**
 * Sets the bucket policy of a Minio bucket.
 * @param {string} bucketName - Name of the bucket.
 * @param {object} policy - Bucket policy (as a JSON object).
 * @returns {Promise} A Promise that resolves when the bucket policy is set or rejects with an error.
 * @link https://min.io/docs/minio/linux/developers/javascript/API.html#setbucketnotification-bucketname-bucketnotificationconfig-callback
 */
MinioDB.prototype.setBucketPolicy = function(bucketName, bucketPolicy) {
  const self = this;
  if (self.debug) {
    debug('setBucketPolicy: %s, %s', bucketName, bucketPolicy);
  }
  return new Promise((resolve, reject) => {
    self.client.setBucketPolicy(bucketName, JSON.stringify(bucketPolicy), function(err, data) {
      if (err) {
        if (self.debug) {
          g.error('{{MinioDB}} connection failed: %s %s', self.settings.endpoint, err);
        }
        reject(err);
      } else {
        resolve(data);
      }
    });
  });
};

