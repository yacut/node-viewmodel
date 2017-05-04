'use strict';

var util = require('util'),
  Repository = require('../base'),
  ViewModel = Repository.ViewModel,
  _ = require('lodash'),
  uuid = require('uuid/v1'),
  async = require('async'),
  aws = Repository.use('aws-sdk'),
  collections = [];

function DynamoDB(options) {
  Repository.call(this, options);

  var defaults = {
    tableName: 'context',
    ttl:  1000 * 60 * 60 * 1, // 1 hour
    endpointConf: {
      endpoint: 'http://localhost:4567' //dynalite
    },
    EventsReadCapacityUnits: 1,
    EventsWriteCapacityUnits: 3,
  };
  if (process.env['AWS_DYNAMODB_ENDPOINT']) {
    defaults.endpointConf.endpoint = process.env['AWS_DYNAMODB_ENDPOINT'];
  }
  _.defaults(options, defaults);
  this.options = options;
}

util.inherits(DynamoDB, Repository);

_.extend(DynamoDB.prototype, {

  connect: function (callback) {
    var self = this;
    self.client = new aws.DynamoDB(self.options.endpointConf);
    self.documentClient = new aws.DynamoDB.DocumentClient(self.client);
    self.isConnected = true;
    function revisionTableDefinition(opts) {
      var def = {
        TableName: opts.tableName,
        KeySchema: [
          { AttributeName: 'id', KeyType: 'HASH' },
          { AttributeName: 'viewmodel', KeyType: 'RANGE' }
        ],
        AttributeDefinitions: [
          { AttributeName: 'id', AttributeType: 'S' },
          { AttributeName: 'viewmodel', AttributeType: 'S' }
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
        error('connect error: ' + err);
        if (callback) callback(err);
      } else {
        self.emit('connect');
        if (callback) callback(null, self);
      }
    });
  },

  disconnect: function (callback) {
    this.emit('disconnect');
    if (callback) callback(null);
  },

  getNewId: function(callback) {
    callback(null, uuid());
  },

  /// FIXME
  get: function (id, callback) {
    if (!id || !_.isString(id)) {
      var err = new Error('Please pass a valid id!');
      return callback(err);
    }
    var self = this;
    var params = {
     Key: {id: {S: id}},
     TableName: this.options.tableName
    };
    this.client.getItem(params, function (err, data) {
      if (err) {
        return callback(err);
      }

      if (!data) {
        return callback(null, new ViewModel({ id: id }, self));
      }

      var vm = new ViewModel(data, self);
      vm.actionOnCommit = 'update';
      callback(null, vm);
    });
  },

  /// FIXME
  find: function(query, queryOptions, callback) {
    this.checkConnection();
    var self = this;
    var params = {
      KeyConditions: getKeyConditions(query),
      ProjectionExpression: 'viewmodel',
      TableName: this.options.tableName
    };

    this.client.query(params, function(err, vms) {
      // Map to view models
      vms = _.map(vms.Items, function(item) {
        var data = JSON.parse(item.viewmodel.S);
        var vm = new ViewModel(data, self);
        vm.actionOnCommit = 'update';
        return vm;
      });

      callback(err, vms);
    });
  },

  /// FIXME
  findOne: function(query, queryOptions, callback) {
    this.checkConnection();
    var self = this;
    var params = {
      KeyConditions: getKeyConditions(query),
      Limit: 1,
      ProjectionExpression: 'viewmodel',
      TableName: this.options.tableName
    };

    this.client.query(params, function(err, vms) {
      if (err) {
        return callback(err);
      }

      if (!vms || !vms.Items || vms.Items.length === 0) {
        return callback(null, null);
      }

      var data = JSON.parse(vms.Items[0].viewmodel.S);
      var vm = new ViewModel(data, self);
      vm.actionOnCommit = 'update';

      callback(err, vm);
    });
  },

  /// FIXME
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
        // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#deleteItem-property
        this.client.deleteItem(params, function(err, data) {
          callback(err);
        });
        break;
      case 'create':
        vm.set('_hash', this.getNewId());
        var params = {
          Item: {
            id: {S: vm.get('id')},
            viewmodel: {S: JSON.stringify(vm.attributes)},
          },
          TableName: this.options.tableName,
          ReturnConsumedCapacity: 'TOTAL',
        };
        // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#putItem-property
        this.client.putItem(params, function(err, data) {
          if (err) {
            return callback(new ConcurrencyError(err));
          }
          vm.actionOnCommit = 'update';
          callback(err, vm);
        });
        break;
      case 'update':
        vm.set('_hash', this.getNewId());
        var params = {
          ExpressionAttributeNames: {
           '#viewmodel': 'viewmodel',
          },
          ExpressionAttributeValues: {
            ':viewmodel': {
              S: JSON.stringify(vm.attributes)
            },
          },
          Key: {
            id: {S: vm.get('id')},
          },
          TableName: this.options.tableName,
          ReturnValues: 'ALL_NEW',
          UpdateExpression: 'SET #viewmodel = :viewmodel'
        };
        // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#updateItem-property
        this.client.updateItem(params, function(err, data) {
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

  /// FIXME
  checkConnection: function() {
    if (this.options.tableName) {
      return;
    }
    if (collections.indexOf(this.options.tableName) < 0) {
      collections.push(this.options.tableName);
    }

    var params = {
      TableName: this.options.tableName
    };
    this.client.waitFor('tableExists', params, function(err, data) {
      if (err) {
        return console.error(err, err.stack);
      }
      console.log('DynamoDB connected.');
    });
  },

  /// FIXME
  clearAll: function (callback) {
    async.each(collections, function(tableName, done){
      this.client.deleteTable({ TableName: tableName}, done);
    }, callback);
  },

  clear: function (callback) {
    this.client.deleteTable({ TableName: this.options.tableName}, callback);
  }

});


var getKeyConditions = function getKeyConditions (query) {
  // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#query-property
  var KeyConditions = {};
  _(query).keys().filter(function (k) {
    return _.isString(query[k]) || _.isNumber(query[k]) || _.isDate(query[k]) || _.isBoolean(query[k]);
  }).each(function (k) {
    KeyConditions[k]={ComparisonOperator:'EQ',AttributeValueList:[{ S: query[k] }]};
  });
  return KeyConditions;
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

  var active = function (d, cbActive) {
    var status = d.Table.TableStatus;
    async.until(
      function () { return status === 'ACTIVE'; },
      function (cbUntil) {
        client.describeTable({ TableName: d.Table.TableName }, function (err, data) {
          if (err) {
            console.error('There was an error checking ' + d.Table.TableName + ' status: ' + JSON.stringify(err, null, 2));
            cbUntil(err);
          } else {
            status = data.Table.TableStatus;
            setTimeout(cbUntil(null, data), 1000);
          }
        });
      },
      function (err, r) {
        if (err) {
          console.error('connect create table error: ' + err);
          return cbActive(err);
        }
        cbActive(null, r);
      });
  };

  async.compose(active, create, exists)(params, function (err, result) {
    if (err) callback(err);
    else callback(null, result);
  });
};

module.exports = DynamoDB;
