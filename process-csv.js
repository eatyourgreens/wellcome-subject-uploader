#!/usr/bin/env node
require('es6-promise').polyfill();

var REPORTS = '/Users/odonnelladmin/Documents/wellcome/MOH for zooniverse/Reports-sorted.csv';
var IMAGEDIR = '/Users/odonnelladmin/Documents/wellcome/MOH for zooniverse/image files/';
var TEXTDIR = '/Users/odonnelladmin/Documents/wellcome/ocr text 2/moh_text/text/';
// production
var PROJECTID = "396";
var WORKFLOWID = "215";
// staging
// var PROJECTID = "908";
// var WORKFLOWID = "1483";

var BOROUGHS = [
  'Battersea (London, England)',
  'Bermondsey (London, England)',
  'Camberwell (London, England)'
];

var csv = require('csv');
var fs = require('fs');
var glob = require('glob')
var Panoptes = require('panoptes-client');
var argv = require('yargs').argv
var request = require('request');

var input = fs.createReadStream( REPORTS );
var api = null;
var count = 0
var files = {};
var priorities = {};

function signIn() {
  login = {
    login: argv.u,
    password: argv.p,
    remember_me: true
  }

  return Panoptes.auth.signIn(login);
}

function processLine(line) {
  var borough = line['BOROUGH'];
  if (BOROUGHS.indexOf( borough ) > -1) {
    var subjectSet = api.type('subject_sets').create({
      display_name: line['File prefix'],
      metadata: line,
      links: {project: PROJECTID}
    });
    count++;
    
    if (count < 4) subjectSet.save()
    .then(function(subjectSet){
      var prefix = line['File prefix']
      return Promise.all( processPages(borough, prefix) );
    })
    .then(function(subjects){
      var links = subjects.map( function(subject){
        return subject.id;
      });
      subjects.forEach( uploadFiles );
      console.info('add links to subject set', subjectSet.id);
      subjectSet.addLink('subjects', links).then(function () {
        setPriorities(subjectSet, links);
      });
      api.type('workflows').get(WORKFLOWID).then(function (workflow) {
        workflow.addLink('subject_sets', [subjectSet.id]);
      });
    })
    .catch(function(error){
      console.info('Error creating subject', error);
    });
  }
}

function processPages(borough, prefix) {
  var promises = []
  var newSubjectIDs = []
  var images = glob.sync(IMAGEDIR + prefix + '_*.jpg');
  console.info(borough, prefix, images.length);
  for (i=0; i < 11; i++) {
    var good = false;
    filename = prefix + '_0_' + i
    subject = api.type('subjects').create({
      locations: ['image/jpeg', 'text/plain'],
      metadata: {
        page: i,
        priority: i, 
        filename: filename,
        image: filename + '.jpg'
      },
      links: {project: PROJECTID}
    });
    image = readImage(filename, subject);
    text = readText(filename, subject);
    if (image && text.length) {
      files[filename] = {'image/jpeg': image, 'text/plain': text};
      promises.push( subject.save().catch(function (error) {
        console.log('Unable to save subject', filename, error);
      }) );
    }
  }
  return promises;
}

function setPriorities (subjectSet, links) {
  console.info('set priorities for subject set', subjectSet.id);
  pages = links.length / 20;
  pages = Math.ceil( pages );
  for (page = pages; page > 0; page--) {
    api.type('set_member_subjects').get({subject_set_id: subjectSet.id.toString(), page: page})
    .then( function (setMemberSubjects) {
      console.info('Linked subjects:', setMemberSubjects.length);
      setMemberSubjects.forEach( function (setMemberSubject) {
        console.log( 'PRIORITY', setMemberSubject.links.subject, parseInt(priorities[setMemberSubject.links.subject]) );
        setMemberSubject.update( {'priority': parseInt(priorities[setMemberSubject.links.subject])} );
        setMemberSubject.save().catch(function(error){
          console.log(setMemberSubject.id, setMemberSubject.priority, setMemberSubject.links);
          console.info('Error setting priority', error);
        });
      });
    })
    .catch(function (error) {
      console.info( 'Error getting SMS', error);
    });
  }
}

function readImage (filename, subject) {
  try {
     return fs.readFileSync(IMAGEDIR + filename + '.jpg');
   }
   catch (e) {
     console.log(filename, 'READ ERROR');
     return false;
   }
}

function readText (filename, subject) {
  try {
    return fs.readFileSync(TEXTDIR + filename + '.txt');
  }
  catch (e) {
    console.log(filename, 'READ ERROR');
    return false
  }
}

function uploadFiles (subject){
  console.info('upload files for ', subject.id);
  priorities[subject.id] = subject.metadata.page;
  subject.locations.forEach(function(location){
    for (var mimeType in location) {
      headers = {'Content-Type': mimeType};
      url = location[mimeType];
      body = files[subject.metadata.filename][mimeType];
      request.put({headers: headers, url: url, body: body}, function (error, message, body) {
          console.info( 'uploaded to S3', message.statusCode, body.length);
          if (error) console.log('Error uploading to S3', subject.metadata.filename, mimeType, error);
        });
    }
  });
  return subject;
}

var parser = csv.parse({columns: true}, function(err, data){
  data.forEach( processLine );
});

signIn()
  .then( function (user) {
    api = Panoptes.apiClient;
    api.update({'params.admin' : user.admin})
    
    Promise.all([
      api.type('workflows').get(WORKFLOWID), 
      api.type('projects').get(PROJECTID)
    ])
      .then( function (values) {
        console.log('WORKFLOW', values[0].display_name);
        console.log('PROJECT', values[1].display_name);
        input.pipe( parser );
      });
  })
  .catch(function(error){
    console.log('Done', error);
  });
