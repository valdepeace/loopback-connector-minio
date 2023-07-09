'use strict';
const assert = require('assert');
const should = require('should');
const it = require('mocha').it;
const before = require('mocha').before;
const after = require('mocha').after;
// const fetch = require('node-fetch');
const loopback = require('loopback');

const MinioDB = require('../lib/minio');
const Docker = require('dockerode');
const { c } = require('strong-globalize/lib/globalize');
const docker = new Docker();

async function startMinioContainer() {
  const image = 'minio/minio';
  const containerName = 'loopback-connector-minio-test';
  const port = 9002;
  const accessKey = 'YOUR_ACCESS_KEY';
  const secretKey = 'YOUR_SECRET_KEY';
  const consolePort = 9003;
  const env = [
    `MINIO_ROOT_USER=${accessKey}`,
    `MINIO_ROOT_PASSWORD=${secretKey}`,
  ];

  const createOptions = {
    Image: image,
    name: containerName,
    Env: env,
    ExposedPorts: {
      '9002/tcp': {},
      '9003/tcp': {},
    },
    HostConfig: {
      PortBindings: {
        '9000/tcp': [{ HostPort: `${port}` }],
        '9003/tcp': [{ HostPort: `${consolePort}` }],
      },
    },
    Cmd: ['server', '/data', '--console-address', `:${consolePort}`],
  };

  try {
    const containers = await docker.listContainers({ all: true });
    const containerExists = containers.find((c) => c.Names.includes(`/${containerName}`));
    if (containerExists) {
      const c = docker.getContainer(containerExists.Id);
      if (containerExists.State === 'running' && containerExists.State !== 'exited') {
        await c.stop();
      }
      await c.remove();
    }
    const container = await docker.createContainer(createOptions);
    await container.start();
    console.log('Minio container started successfully.');
    console.log(`Minio URL: http://localhost:${port}`);
  } catch (error) {
    console.error('Error starting Minio container:', error);
  }
}
async function waitForMinioServer() {
  const serverPort = 9003;
  return new Promise((resolve) => {
    const checkServer = async () => {
      try {
        const fetch = await import('node-fetch');
        const response = await fetch.default(`http://localhost:${serverPort}`);
        if (response.ok) {
          console.log('Minio server is ready.');
          resolve();
        } else {
          // Si el servidor no está listo, esperar 1 segundo y verificar nuevamente
          setTimeout(checkServer, 1000);
        }
      } catch (error) {
        // Si hay un error al conectar al servidor, esperar 1 segundo y verificar nuevamente
        setTimeout(checkServer, 1000);
      }
    };

    checkServer();
  });
}
startMinioContainer();

const minioConfigRemote = {
  endPoint: 'play.min.io',
  port: 9000,
  useSSL: true,
  accessKey: 'Q3AM3UQ867SPQQA43P2F',
  secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG',
};
const minioConfigLocal = {
  endPoint: 'localhost',
  port: 9002,
  useSSL: false,
  accessKey: 'YOUR_ACCESS_KEY',
  secretKey: 'YOUR_SECRET_KEY',
};
let minioConfig = minioConfigLocal;
if (process.env.REMOTE_TEST) {
  minioConfig = minioConfigRemote;
  minioConfig.serverRegion = 'us-east-1';
}

describe('MinioDB', function () {
  let lb4App;

  before(startLB4App);
  after(stopLB4App);
  describe('Validation settings', () => {
    it('creates ` client` upon datasource creation', async () => {
      const ds = await createDataSource(minioConfig);
      ds.connector.should.have.property('client');
      ds.settings.should.have.property('endPoint');
      ds.settings.should.have.property('port');
      // ds.connector.client.should.have.property('apis');
    });
  });
  describe('functional tests', () => {
    let ds, Todo;
    const bucketName = 'test';
    const objectName = 'test-object.txt';
    const data = 'This is the content of the object.';
    before(async () => {
      ds = await createDataSource(minioConfig);
      Todo = ds.createModel('Todo', {}, { base: 'Model' });
    });
    it('creates models with method of minio', function () {
      (typeof Todo.getObject).should.eql('function');
      (typeof Todo.listBuckets).should.eql('function');
      (typeof Todo.makeBucket).should.eql('function');
    });
    it('Create bucket', (done) => {
      const bucketName = 'test';
      Todo.makeBucket(bucketName, minioConfig.serverRegion).then((res) => {
        done();
      }, (err) => {
        if (err) {
          if (err.code == 'BucketAlreadyOwnedByYou') {
            console.log('Bucket already exists');
            return done();
          } else {
            done(err)            
          }
        }
      });
    });
    it('puts an object in the bucket', async () => {
      const bucketName = 'test';
      return Todo.putObject(bucketName, objectName, data).then((res) => {
        assert.ok(res.hasOwnProperty('etag'));        
      }, err => {              
        assert.fail(err)
      })
    });

    it('gets an object from the bucket', (done) => {
      const objectName = 'test-object.txt';
      Todo.getObject(bucketName, objectName, (err, dataStream) => {
        if (err) {
          throw err;
        } else {
          let objectData = '';
          dataStream.on('data', (chunk) => {
            objectData += chunk;
          });
          dataStream.on('end', () => {
            assert.strictEqual(objectData, 'This is the content of the object.');
            done();
          });
        }
      });
    });

    it('gets the object stats', () => {
      Todo.statObject(bucketName, objectName).then((stat)=>{
        assert.strictEqual(stat.size, 34);
      }, (err) => {
        assert.fail(err);
      });
    });

    it('removes the object from the bucket', (done) => {
      Todo.removeObject(bucketName, objectName).then(()=>{
        done();
      }, (err) => {
        done(err);        
      });
    });

    // it('removes the bucket', (done) => {
    //   Todo.removeBucket(bucketName, (err) => {
    //     if (err) {
    //       throw err;
    //     } else {
    //       done();
    //     }
    //   });
    // });
  });

  async function startLB4App() {
    const TodoListApplication = require('@loopback/example-todo')
      .TodoListApplication;
    const config = {
      rest: {
        port: 0,
        host: '127.0.0.1',
        openApiSpec: {
          // useful when used with OASGraph to locate your application
          setServersFromRequest: true,
        },
      },
    };
    lb4App = new TodoListApplication(config);
    lb4App.bind('datasources.config.db').to({ connector: 'memory' });
    await lb4App.boot();
    await lb4App.start();
  }

  async function stopLB4App() {
    if (!lb4App) return;
    await lb4App.stop();
  }
  // Agrega más pruebas para los demás métodos de la biblioteca MinioDB
});
async function createDataSource(options) {
  await waitForMinioServer();
  return new Promise((resolve, reject) => {
    const config = Object.assign(
      {
        connector: require('../index'),
      },
      options,
    );
    try {
      const ds = loopback.createDataSource('minio', config);
      ds.connected = true;
      resolve(ds);
    } catch (err) {
      console.log(err);
      reject(err);
    }
  });
}
