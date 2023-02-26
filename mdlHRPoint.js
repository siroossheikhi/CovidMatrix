'use strict';
const mongoose = require('mongoose');
require('mongoose-long')(mongoose);
const Types = mongoose.Schema.Types;
const ObjectId = mongoose.Types.ObjectId;
const Long = mongoose.Types.Long;
const {helper, patterns, exception, constants} = require('../../libs');
const moment = require('moment');
const mdlShared = require('./mdlShared');
const haversine = require('haversine');
const shmHRPoint = new mongoose.Schema({
  hrpoint_itime: {type: Types.Long, required: true, match: patterns.UnixTimeStamp, default: () => moment().unix()},
  hrpoint_title: {type: String, required: true, match: patterns.MediumString},
  hrpoint_locpoint: {type: mdlShared.shmGeoJson, required: true},
  hrpoint_radius: {type: Number, required: true, min: constants.HRPoint_RadiusMin, max: constants.HRPoint_RadiusMax},
  hrpoint_risk: {type: Number, required: true, min: constants.HRPoint_RiskMin, max: constants.HRPoint_RiskMax},
}, { versionKey: false });
shmHRPoint.index({hrpoint_locpoint: '2dsphere'});
/**
 * Add batch records
 * @param {Array} args: array of object: title, locpoint, radius, risk, max 1000 items per request
 * @param {Boolean} preserve: Preserve before inserted location points
 * @param {Function} callback
 */
module.exports.addBatch = function(local, args, preserve = true, callback = null, dbSession = null){
  let check = true;
  check = check && Array.isArray(args) && 
  (args.length >= constants.HRPoint_AddBatchMin) && 
  (args.length <= constants.HRPoint_AddBatchMax);
  if(check){
    for(let i = 0; i < args.length; i++){
      let point = args[i];
      if(check){
        check = check && point.title && patterns.MediumString.test(args.title);
        check = check && point.locpoint && Array.isArray(point.locpoint);
        check = check && point.locpoint.length === 2;
        check = check && point.locpoint[0] && patterns.Longitude.test(point.locpoint[0]);
        check = check && point.locpoint[1] && patterns.Latitude.test(point.locpoint[1]);
        check = check && point.radius && patterns.Integer.test(point.radius);
        check = check && point.radius >= constants.HRPoint_RadiusMin && point.radius <= constants.HRPoint_RadiusMax;
        check = check && point.risk && patterns.Integer.test(point.risk);
        check = check && point.risk >= constants.HRPoint_RiskMin && point.risk <= constants.HRPoint_RiskMax;
      }else{
        break;
      }
    }
  }
  if(check){
    new Promise((resolve, reject) => {
      if(preserve){
        resolve();
      }else{
        this.truncate(local, (err, success) => {
          if(success){
            resolve();
          }else{
            reject(err);
          }
        }, dbSession);
      }
    })
    .then(() => {
      let points = args.map(point => ({
        hrpoint_title: point.title, 
        hrpoint_locpoint: {type: mdlShared.cnstPoint, coordinates: point.locpoint}, 
        hrpoint_radius: point.radius, 
        hrpoint_risk: point.risk
      }));
      local.dBase.ins.models[cstMdlName].create(points, {session: dbSession})
      .then(doc => {
        callback && callback(null, true);
      }).catch(err => {
        callback && callback(err, false);
      });
    })
    .catch(err => {
      callback && callback(err, false);
    });
  }else{
    callback && callback(exception.throwWrongDataFormat(local.Strings), false);
  }
}
/**
 * Erase the records
 * @param {Function} callback
 */
module.exports.truncate = function(local, callback = null, dbSession = null){
  local.dBase.ins.models[cstMdlName].deleteMany({}, {session: dbSession}).exec()
  .then(() => {
    callback && callback(null, true);
  })
  .catch(err => {
    callback && callback(err, false);
  });
}
/**
 * Get one point near location
 * @param {*} local 
 * @param {*} locpoint
 * @param {*} callback 
 */
module.exports.getNearPoint = function(local, locpoint, callback = null){
  let check = true;
  check = check && locpoint && Array.isArray(locpoint) && locpoint.length === 2;
  check = check && locpoint[0] && patterns.Longitude.test(locpoint[0]);
  check = check && locpoint[1] && patterns.Latitude.test(locpoint[1]);
  if(check){
    let point = {
      type: mdlShared.cnstPoint,
      coordinates: [locpoint[0], locpoint[1]]
    };
    local.dBase.ins.models[cstMdlName].aggregate([
      {
        $geoNear: {
          near: point,
          distanceField: "distance",
          maxDistance: constants.HRPoint_RadiusMax
        }
      },
      { $limit: 1 }
    ]).exec(function(err, doc){
      if(err){
        callback && callback(err, null);
      }else if (doc && Array.isArray(doc) && doc.length === 1){
        doc = doc[0];
        if(doc.distance <= doc.hrpoint_radius){
          callback && callback(null, doc);
        }else{
          callback && callback(null, null);
        }
      }else{
        callback && callback(null, null);
      }
    });
  }else{
    callback && callback(exception.throwWrongDataFormat(local.Strings), null);
  }
}
/**
 * Get near points
 * @param {Object} args: locpoint: Array, grvpoint: Array, delta: float
 * @param {Function} callback
 */
module.exports.getNearPoints = function(local, args, callback = null){
  let check = true;
  if(args.locpoint){
    check = check && args.locpoint && Array.isArray(args.locpoint) && args.locpoint.length === 2;
    check = check && args.locpoint[0] && patterns.Longitude.test(args.locpoint[0]);
    check = check && args.locpoint[1] && patterns.Latitude.test(args.locpoint[1]);
  }
  check = check && args.grvpoint && Array.isArray(args.grvpoint) && args.grvpoint.length === 2;
  check = check && args.grvpoint[0] && patterns.Longitude.test(args.grvpoint[0]);
  check = check && args.grvpoint[1] && patterns.Latitude.test(args.grvpoint[1]);
  check = check && args.delta && patterns.Delta.test(args.delta);
  if(check){
    let point = {
      type: mdlShared.cnstPoint,
      coordinates: [args.grvpoint[0], args.grvpoint[1]]
    };
    let maxDistance = helper.delta2Meter(args.delta);
    local.dBase.ins.models[cstMdlName].aggregate([
      {
        $geoNear: {
          near: point,
          distanceField: "distance",
          distanceMultiplier: 0.001,
          maxDistance: maxDistance
        }
      },
      { $limit: constants.HRPoint_NearByMaxPoints }
    ]).exec(function(err, doc){
        if(err){
          callback && callback(err, null);
        }else{
          let points = [];
          doc.forEach(point => {
            let pTime = moment.unix(point.hrpoint_itime);
            let distance = null;
            if(args.locpoint){
              distance = haversine(args.locpoint, point.hrpoint_locpoint.coordinates, {unit: 'km', format: '[lon,lat]'});
              if(distance != 0){
                distance = (distance <= constants.HRPoint_MaxDistance)? distance.toFixed(2) : distance.toFixed(0);
              }
              distance = distance.toString();
            }
            points.push({
              time: pTime.fromNow(),
              title: point.hrpoint_title,
              locpoint: [point.hrpoint_locpoint.coordinates[0], point.hrpoint_locpoint.coordinates[1]],
              radius: point.hrpoint_radius,
              risk: point.hrpoint_risk,
              distance: distance,
            });
          });
          callback && callback(null, points);
        }
    });
  }else{
    callback && callback(exception.throwWrongDataFormat(local.Strings), null);
  }
}
//Initializer
const cstMdlName = 'mdlHRPoint';
const cstDocName = 'doc_hrpoint';
module.exports.initial = function(dBase){
  let mdlHRPoint = dBase.ins.model(cstMdlName, shmHRPoint, cstDocName);
  dBase.ins.db.listCollections({name: cstDocName}).next((err, info) => {
    if (!err && !info) {
      mdlHRPoint.createCollection();
    }
  });
}
// let args = [
//   {title:"USP University",locpoint: [-47.898274,-22.002302],radius:5000,risk:10},
//   {title:"Pereire Lopes",locpoint: [-47.900327,-22.002320],radius:1000,risk:5},
//   {title:"Parque do Kartódromo",locpoint: [-47.898825,-21.998878],radius:2000,risk:10},
//   {title:"Cemitério Nossa Senhora do Carmo",locpoint: [-47.890929, -21.998400],radius:1000,risk:10},
//   {title:"Praça Geraldo Eugenio T. Pizza",locpoint: [-47.894427, -21.999275],radius:1000,risk:10}
// ];
// this.addBatch(local, args, false);