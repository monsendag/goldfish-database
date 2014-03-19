#!/usr/bin/env node-theseus

var MongoClient = require('mongodb').MongoClient;
var _ = require('lodash');
var moment = require('moment');
var fs = require('fs');
var csv = require('csv');



MongoClient.connect('mongodb://127.0.0.1:27017/yow', function (err, db) {
  if (err) throw err;

  var file = 'datasets/yow-userstudy/yow_userstudy_raw reordered.csv';
  
  var csvReader = csv().from.path(file, {
    delimiter: ',',
    escape: '"'
  });

  var collection = db.collection('events');
  var header;

  csvReader
    .on('error', function (error) {
    console.log(error.message);
  })
    .on('record', function (row, index) {
    if (index === 0) {
      header = _.map(row, function(col) { return col.replace(/^\d+\-/, ''); });
      console.log(row);
    } else {
      row = row.map(function (value, index) {
        switch (index) {
          // parse TimeVisit
          case 16:
            return moment(value, "DD/MM/YY HH:mm").toDate();
            // parse ServerTimeVisit
          case 17:
            return moment(value, "DD/MM/YY HH:mm").toDate();
            // parse classes / categories
            // split on |, then trim each value, then filter out empty values
          case 21:
            return _.chain(value.split('|')).invoke('trim').filter(function(val) { return val.length>0; }).valueOf();
            // parse the rest as integers
          default:
            return parseInt(value, 10);
        }
      });
      var record = _.zipObject(header, row);

      console.log(record);
      collection.insert(record, function (err, docs) {
        if (err) throw err;
      });
    }

  });
});