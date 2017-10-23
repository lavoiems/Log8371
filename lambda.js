const _ = require('lodash');
const AWS = require('aws-sdk');
AWS.config.setPromisesDependency(Promise);

const MIN_N_WORKERS = 51;
let total_workers = 0;

function invoke(file, offset, n_iter) {
  console.log("Starting worker");
  const lambda = new AWS.Lambda({ apiVersion: '2015-03-31', region: 'ca-central-1' });
  const params = {
    FunctionName: 'countOccurence',
    InvocationType: 'RequestResponse',
    Payload: JSON.stringify({"file": file, "offset": offset, "n_iter": n_iter})
  };
  return lambda
    .invoke(params)
    .promise()
}


function split(content, maxNiters) {
  const fileName = content['Key'];
  const fileNiters = Math.ceil(content['Size'] / 50000000);
  const promises = _.range(0, fileNiters, maxNiters)
    .map(offset => {
      total_workers += 1;
      const nIter = Math.min(fileNiters - offset, maxNiters);
      return invoke(fileName, offset, nIter);
    });
  return Promise.all(promises)
}


function aggregate(initTime) {
  console.log("Total workers: " + total_workers);
  const invTime = (new Date()).getTime();
  console.log("Invokation time: " + invTime - initTime);
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  let allItems = {};

  s3
    .listObjectsV2({Bucket: 'log4410-results-lambda'})
    .promise()
    .then(contents => {
      const fn = contents['Contents'].map(content => () => get(allItems, content));
      return process(fn);
    })
    .then(() => {
      const endTime = (new Date()).getTime();
      const execTime = endTime - initTime;
      console.log("Total execution time: " + execTime);
    })
    .catch(err => console.log(err));
}

function process(fns, res = []) {
  if (fns.length === 0) return Promise.resolve(res);

  const head = _.head(fns);
  const tail = _.tail(fns);

  return head().then(newRes => process(tail, newRes));//res.push(newRes)));
}

function get(allItems, content) {
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  return s3
    .getObject({ Bucket: 'log4410-results-lambda', Key: content['Key'] })
    .promise()
    .then(s3Obj => {
      const countDict = s3Obj['Body'].toString('utf-8');
      _.mergeWith(allItems, countDict, (obj, src) => obj + src);
    });
}


function execute(max_n_iters) {
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  s3.listObjectsV2({Bucket: "log4410-enwiki"}).promise()
    .then(contents => {
      const initTime = (new Date()).getTime();
      const promises = _.map(contents['Contents'], content =>
          split(content, max_n_iters)
      );
      return Promise.all(promises)
        .then(scores => aggregate(initTime))
        .catch(err => console.log(err));
    });
}

execute(20);
