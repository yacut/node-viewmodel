'use strict';
// TODO
// - on commit update need to check if update is nessesary (see inmemory implementation)
// - find(One) method should support mongo like expressions ($gte, $lte, $or, $and, $regex)
// - remove all dynamodb changes in tests
// - remove changes in config files (eslint)
var util = require('util'),
  Repository = require('../base'),
  ViewModel = Repository.ViewModel,
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  debug = require('debug')('repository:dynamodb'),
  aws = Repository.use('aws-sdk'),
  collections = ['context'];

function DynamoDB(options) {
  Repository.call(this, options);

  var defaults = {
    tableName: 'context',
    ttl:  1000 * 60 * 60 * 1, // 1 hour
    endpointConf: {
      endpoint: 'http://localhost:4567' //dynalite
    },
    // heartbeat: 1000
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  debug('region', aws.config.region);
  if(!aws.config.region){
    aws.config.update({region: 'us-east-1'});
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Repository);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB();
    debug('Client created.');
    function revisionTableDefinition(opts) {
      var def = {
        TableName: opts.tableName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: opts.EventsReadCapacityUnits || 5,
          WriteCapacityUnits: opts.EventsWriteCapacityUnits || 5
        }
      };

      return def;
    }
    createTableIfNotExists(self.client, revisionTableDefinition(self.options), function (err) {
      if (err) {
        console.error('connect error: ' + err);
        if (callback) callback(err);
      } else {
        self.emit('connect');
        self.isConnected = true;
        debug('Connected.');

        if (self.options.heartbeat) {
          self.startHeartbeat();
        }
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function (callback) {
    this.emit('disconnect');
    debug('Disconnected.');
    if (callback) callback(null);
  },

  getNewId: function(callback) {
    callback(null, uuid());
  },

  get: function (id, callback) {
    this.checkConnection();

    if(_.isFunction(id)) {
      callback = id;
      id = null;
    }

    if (!id) {
      id = uuid();
    }

    var self = this;
    var params = {
      Key: {id: {S: id}},
      ProjectionExpression: 'viewmodel',
      TableName: this.options.tableName
    };
    this.client.getItem(params, function (err, data) {
      debug('get err', err);
      debug('get data', JSON.stringify(data, null, 2));
      if (err) {
        return callback(err);
      }

      if (!data|| !data.Item || !data.Item.viewmodel) {
        return callback(null, new ViewModel({ id: id }, self));
      }

      var vm = new ViewModel(JSON.parse(data.Item.viewmodel.S), self);
      vm.actionOnCommit = 'update';
      callback(null, vm);
    });
  },

  find: function(query, queryOptions, callback) {
    this.checkConnection();
    var self = this;
    var params = _.defaults({
      TableName: this.options.tableName
    }, getFilterExpressions(query));

    if(queryOptions.limit) {
      params.Limit = queryOptions.limit;
    }

    this.client.scan(params, function(err, data) {
      debug('find err', err);
      debug('find params', JSON.stringify(params, null, 2));
      debug('find query', JSON.stringify(query, null, 2));
      debug('find queryOptions', JSON.stringify(queryOptions, null, 2));
      debug('find data', JSON.stringify(data, null, 2));
      if(!data || !data.Items) {
        callback(err, []);
      }
      var vms = _.map(data.Items, function(item) {
        var data = JSON.parse(item.viewmodel.S);
        var vm = new ViewModel(data, self);
        vm.actionOnCommit = 'update';
        return vm;
      });

      callback(err, vms);
    });
  },

  findOne: function(query, queryOptions, callback) {
    this.checkConnection();
    var self = this;
    var params = _.defaults({
      TableName: this.options.tableName,
    }, getFilterExpressions(query));

    this.client.scan(params, function(err, vms) {
      debug('findOne err', err);
      debug('findOne params', JSON.stringify(params, null, 2));
      debug('findOne vms', JSON.stringify(vms, null, 2));
      if (err) {
        return callback(err);
      }
      if(!vms || !vms.Items || vms.Items.length === 0 || !vms.Items[0].viewmodel) {
        return callback(null, null);
      }

      var data = JSON.parse(vms.Items[0].viewmodel.S);
      var vm = new ViewModel(data, self);
      vm.actionOnCommit = 'update';

      callback(err, vm);
    });
  },

  commit: function(vm, callback) {
    this.checkConnection();

    if(!vm.actionOnCommit) return callback(new Error());

    switch(vm.actionOnCommit) {
      case 'delete':
        if (!vm.has('_hash')) {
          return callback(null);
        }
        var params = {
          Key: {
            id: {S: vm.id},
          },
          TableName: this.options.tableName,
        };
        debug('delete item', JSON.stringify(params, null, 2));
        this.client.deleteItem(params, callback);
        break;
      case 'create':
        vm.set('_hash', uuid());
        var params = {
          Item: {
            id: {S: vm.get('id')},
            viewmodel: {S: JSON.stringify(vm.attributes)},
          },
          TableName: this.options.tableName,
          ReturnConsumedCapacity: 'TOTAL',
        };
        debug('create item', JSON.stringify(params, null, 2));
        this.client.putItem(params, function(err, data) {
          if (err) {
            return callback(new ConcurrencyError(err));
          }
          vm.actionOnCommit = 'update';
          callback(err, vm);
        });
        break;
      case 'update':
        vm.set('_hash', uuid());
        var params = {
          Item: {
            id: {S: vm.get('id')},
            viewmodel: {S: JSON.stringify(vm.attributes)},
          },
          TableName: this.options.tableName,
          ReturnConsumedCapacity: 'TOTAL',
        };
        debug('update item', JSON.stringify(params, null, 2));
        this.client.putItem(params, function(err, data) {
          if (err) {
            return callback(new ConcurrencyError(err));
          }
          vm.actionOnCommit = 'update';
          callback(err, vm);
        });
        break;
      default:
        return callback(new Error());
    }
  },

  checkConnection: function() {
    if (!this.collectionName) {
      return;
    }
    if (collections.indexOf(this.collectionName) < 0) {
      collections.push(this.collectionName);
    }

    waitForTableExists(this.client, this.collectionName, function(err, data) {
      if (err) {
        return console.error(err, err.stack);
      }
    });
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (dynamodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      waitForTableExists(self.client, self.options.collectionName, function (err) {
        if (graceTimer) clearTimeout(graceTimer);
        if (err) {
          console.error(err.stack || err);
          self.disconnect();
        }
      });
    }, this.options.heartbeat);
  },

  clearAll: function (callback) {
    debug('clear all');
    var self = this;
    async.each(collections, function(tableName, done) {
      clearTable(self.client, tableName, done);
    }, callback);
  },

  clear: function (callback) {
    clearTable(this.client, this.options.tableName, callback);
  }

});

var clearTable = function(client, tableName, callback) {
  debug('clearTable', tableName);
  async.waterfall([
    function(done){
      client.scan({ TableName: tableName }, function(err, data){
        if(err && err.code === 'ResourceNotFoundException') {
          return done(null, null);
        }
        done(err, data);
      });
    },
    function(data, done){
      debug('clear each', JSON.stringify(data, null, 2));
      if(!data || !data.Items || data.Count === 0) {
        return done();
      }
      async.each(data.Items, function(entry, next){
        debug('delete item', JSON.stringify(entry, null, 2));
        var params = {
          Key: {
            id: {S: entry.id.S}
          },
          TableName: tableName,
          ReturnConsumedCapacity: 'TOTAL'
        };
        client.deleteItem(params, next);
      }, done);
    }
  ], callback);
};

var waitForTableExists = function(client, tableName, callback) {
  debug('Wating for table:', tableName);
  var params = {
    TableName: tableName
  };
  client.waitFor('tableExists', params, callback);
};

var getFilterExpressions = function (query) {
  var filterExpressions = [];
  var ExpressionAttributeValues = {};
  var i = 1;
  _(query).keys().each(function (k) {
    var keyArray = k.split('.');
    var subObj = '';
    var value = query[k];
    if(keyArray.length > 1) {
      k = keyArray.pop();
      subObj = keyArray.join('.');
      filterExpressions.push('contains(viewmodel, :f' + i + ')');
      ExpressionAttributeValues[':f' + i] = { S: subObj };
      filterExpressions.push('contains(viewmodel, :p' + i + ')');
      ExpressionAttributeValues[':p' + i] = { S: '\"' + k + '\":\"' + value + '\"' };
    } else if(_.isObject(value)) {
      if(value.$gte) {
        filterExpressions.push('viewmodel.' + k + ' >= :g' + i);
        ExpressionAttributeValues[':g' + i] = { S: _.toString(value.$gte) };
      }
      if(value.$lte) {
        filterExpressions.push('viewmodel.' + k + ' <= :l' + i);
        ExpressionAttributeValues[':l' + i] = { S: _.toString(value.$lte) };
      }
      if(value.$and) {
        _(value.$and).each(function (andExp) {
          var andKey = _.head((andExp).keys());
          filterExpressions.push('viewmodel.' + andExp + ' = :a' + i);
          ExpressionAttributeValues[':a' + i] = { S: _.toString(andExp[andKey]) };
        });
      }
      if(value.$or) {
        var orFilterExpressions = [];
        _(value.$or).each(function (orExp) {
          var orKey = _.head((orExp).keys());
          orFilterExpressions.push('viewmodel.' + orExp + ' = :o' + i);
          ExpressionAttributeValues[':o' + i] = { S: _.toString(orExp[orKey]) };
        });
        filterExpressions.push(orFilterExpressions.join(' or '));
      }
    } else {
      filterExpressions.push('contains(viewmodel, :p' + i + ')');
      ExpressionAttributeValues[':p' + i] = { S: '\"' + k + '\":\"' + value + '\"' };
    }
    i++;
  });
  if(filterExpressions.length === 0){
    return {};
  }
  return {
    FilterExpression: filterExpressions.join(' and '),
    ExpressionAttributeValues: ExpressionAttributeValues,
  };
};

var createTableIfNotExists = function (client, params, callback) {
  var exists = function (p, cbExists) {
    client.describeTable({ TableName: p.TableName }, function (err, data) {
      if (err) {
        if (err.code === 'ResourceNotFoundException') {
          cbExists(null, { exists: false, definition: p });
        } else {
          console.error('Table ' + p.TableName + ' doesn\'t exist yet but describeTable: ' + JSON.stringify(err, null, 2));
          cbExists(err);
        }
      } else {
        cbExists(null, { exists: true, description: data });
      }
    });
  };

  var create = function (r, cbCreate) {
    if (!r.exists) {
      client.createTable(r.definition, function (err, data) {
        if (err) {
          console.error('Error while creating ' + r.definition.TableName + ': ' + JSON.stringify(err, null, 2));
          cbCreate(err);
        } else {
          cbCreate(null, { Table: { TableName: data.TableDescription.TableName, TableStatus: data.TableDescription.TableStatus } });
        }
      });
    } else {
      cbCreate(null, r.description);
    }
  };

  async.parallel([
    function(done) {
      async.compose(create, exists)(params, done);
    },
    function(done){
      waitForTableExists(client, params.TableName, done);
    }
  ], function (err) {
    debug('createTableIfNotExists', JSON.stringify(err, null, 2));
    if (err) callback(err);
    else callback(null);
  });
};

module.exports = DynamoDB;
