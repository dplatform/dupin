var dbOpen = null;
var dbOffset = 0;
var DEFAULT_COUNT = 15;

function dupin_welcome() {
  var id=null;

  id = document.getElementById('contentTitle');
  id.innerHTML = 'Welcome!';

  id = document.getElementById('contentBody');
  id.innerHTML = '';
}
function dupin_init() {
  dupin_status();
  dupin_database_refresh();
  dupin_welcome();
}

function dupin_status() {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/_status', true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      var id=document.getElementById('footer');

      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        var text = '';
        text += '<p>Start Server TimeVal: sec(' + obj.startTimeVal.sec + ') - usec(' + obj.startTimeVal.usec + ')</p>';
        text += '<p>Current Server TimeVal: sec(' + obj.thisTimeVal.sec + ') - usec(' + obj.thisTimeVal.usec + ')</p>';
        text += '<p>Number of threads: ' + obj.threads + '</p>';
        text += '<p>Number of clients: ' + obj.clients + '</p>';
        text += '<p>Limits: maxHeaders(' + obj.limits.maxHeaders + 
                ') - maxClients(' + obj.limits.maxClients + 
                ') - maxContentLength(' + obj.limits.maxContentLength + 
                ') - clientsForThread(' + obj.limits.clientsForThread + 
                ') - timeout(' + obj.limits.timeout +
                ') - timeoutForThread(' + obj.limits.timeoutForThread + ')</p>';
        text += '<p>Httpd: interface(' + obj.httpd.interface +
                ') - port(' + obj.httpd.port + 
		') - listen(' + obj.httpd.listen + 
                ') - ipv6(' + obj.httpd.ipv6 + ')</p>';

        id.innerHTML = text;
      } 
      catch(e) {
        id.innerHTML='<p>Server error: ' + e + '</p>';
      }

      window.setTimeout(dupin_status, 3000);
    }
  }

  xmlhttp.send(null);
}

function dupin_database_refresh() {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/_all_dbs', true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      var id=document.getElementById('databases');

      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        var text = '';

        for(var i=0; i<obj.length; i++) {
           text += "<div class='db'>" + 
                   "<a href='#' onclick='dupin_database_open(\"" + encodeURI(obj[i]) + "\");'>" +
                   "<img src='open.png' title='Open this database' />" +
                   "</a>" +
                   "<a href='#' onclick='dupin_database_remove(\"" + encodeURI(obj[i]) + "\");'>" +
                   "<img src='remove.png' title='Remove this database' />" +
                   "</a>" +
                   obj[i] +
                   "</div>";
        }

        id.innerHTML=text;
      } 
      catch(e) {
      }
    }
  }

  xmlhttp.send(null);
}

function dupin_database_create() {
  var db = prompt("Insert the name of the new database");
  if(!db) return;

  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('PUT', '/' + encodeURI(db), true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');

        if(obj['ok'] == true) {
          dupin_database_refresh();
          dupin_database_open(db);
        }
      
      } catch(e) {
        alert("Error creating the new database:\n\n"+ xmlhttp.responseText);
      }
    }
  }

  xmlhttp.send(null);
}

function dupin_database_remove(db) {
  if(confirm("Sure to remove this database?") == false)
    return;

  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('DELETE', '/' + encodeURI(db), true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        if(obj['ok'] == true) {
          dupin_database_refresh();

          if(dbOpen == db) {
            dupin_welcome();
            dbOpen = null;
            dbOffset = 0;
          }
        }

      } catch(e) {
        alert("Error deleting the database:\n\n"+ xmlhttp.responseText);
      }
    }
  }

  xmlhttp.send(null);
}

function dupin_database_open(db) {
  dbOpen = db;
  dbOffset = 0;

  dupin_database_show();
}

function dupin_database_show() {
  var id=null;

  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/' + encodeURI(dbOpen), true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');

        id = document.getElementById('contentTitle');
        id.innerHTML = dbOpen + " doc(" + obj['doc_count'] + 
                                ") - deleted(" + obj['doc_del_count'] + ")";

      } catch(e) {
        alert("Error showing database:\n\n"+ xmlhttp.responseText);
      }

      dupin_database_show_record();
    }
  }

  xmlhttp.send(null);
}

function dupin_database_show_record() {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/' + encodeURI(dbOpen) + '/_all_docs?count=' + DEFAULT_COUNT + '&offset=' + dbOffset, true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        var id = document.getElementById('contentBody');

        var text = '<table id="record">' +
                   '  <tr>' +
                   '    <td><b>ID</b></td>' +
                   '    <td><b>Rev</b></td>' +
                   '    <td><b>Object</b></td>' +
                   '    <td><b>Operation</b></td>' +
                   '  </tr>';
                   

        for(var i=0; i<obj['rows'].length; i++) {
          text += '<tr>' +
                  '  <td>' +
                  obj['rows'][i]['_id'] +
                  '  </td>' +
                  '  <td>' +
                  obj['rows'][i]['_rev'] +
                  '  </td>' +
                  '  <td>' +
                  toJsonString(obj['rows'][i]) +
                  '  </td>' +
                  '  <td>' +
                  '   <a href="#" onclick="dupin_record_delete(\'' + obj['rows'][i]['_id'] + '\');"><img src="remove.png" title="Delete this record." /></a>' +
                  '   <a href="#" onclick="dupin_record_update(\'' + obj['rows'][i]['_id'] + '\', \'' + obj['rows'][i]['_rev'] + '\');"><img src="update.png" title="Update this record." /></a>' +
                  '   <a href="#" onclick="dupin_record_show(\'' + obj['rows'][i]['_id'] + '\');"><img src="open.png" title="Show this record." /></a>' +
                  '  </td>' +
                  '</tr>';
        }

        text += '</table>' +
                '<table><tr>' +
                '<td><a href="#" onclick="if(dbOffset > 0) dbOffset-=DEFAULT_COUNT; dupin_database_show();">Prev</a></td>' +
                '<td style="text-align: center;"><a href="#" onclick="dupin_record_new();"><img src="add.png" title="Add a new record." /></a></td>' +
                '<td style="text-align: right;"><a href="#" onclick="dbOffset+=DEFAULT_COUNT; dupin_database_show();">Next</a></td>' +
                '</tr></table>';

        id.innerHTML = text;
      } catch(e) {
      }
    }
  }

  xmlhttp.send(null);
}

function dupin_record_delete(record) {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/' + encodeURI(dbOpen) + '/' + encodeURI(record), true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        if(!obj['_deleted'] || obj['_deleted'] == false) {
          dupin_record_delete_real(record);
          dupin_database_show();
        }

        else
          alert("This record is already deleted.");
      } catch(e) {
      }
    }
  }

  xmlhttp.send(null);
}

function dupin_record_delete_real(record) {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('DELETE', '/' + encodeURI(dbOpen) + '/' + encodeURI(record), true);
  xmlhttp.send(null);
}

function dupin_record_new() {
  var obj = prompt("Insert the JSON object");
  if(!obj) return;

  try {
    var o = eval('(' + obj + ')');
  } catch(e) {
     alert("It is not a valid object.");
     return;
  }

  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('POST', '/' + encodeURI(dbOpen), true);
  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        if(obj['ok'] == true && obj['_id']) {
          dupin_database_show();
          alert("New ID: " + obj['_id']);
        }
      } catch(e) {
      }
    }
  }
  xmlhttp.send(obj);
}

function dupin_record_update(record, rev) {
  var obj = prompt("Insert the JSON object");
  if(!obj) return;

  try {
    var o = eval('(' + obj + ')');
    o["_rev"] = parseInt(rev);
    o["_id"] = record;
    obj = toJsonString(o);
  } catch(e) {
     alert("It is not a valid object.");
     return;
  }

  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('PUT', '/' + encodeURI(dbOpen) + '/' + encodeURI(record), true);
  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        if(obj['ok'] == true && obj['_rev']) {
          dupin_database_show();
          alert("New revision: " + obj['_rev']);
        }
      } catch(e) {
      }
    }
  }
  xmlhttp.send(obj);
}

function dupin_record_show(record) {
  var xmlhttp = new XMLHttpRequest();
  xmlhttp.open('GET', '/' + encodeURI(dbOpen) + '/' + encodeURI(record) + '?revs=true', true);

  xmlhttp.onreadystatechange=function() {
    if (xmlhttp.readyState==4) {
      try {
        var obj = eval('(' + xmlhttp.responseText + ')');
        var id = null;

        id = document.getElementById('contentTitle');
        id.innerHTML = "Record: " + record;

        id = document.getElementById('contentBody');

        var text = '<table id="record">' +
                   '  <tr>' +
                   '    <td><b>ID</b></td>' +
                   '    <td><b>Rev</b></td>' +
                   '    <td><b>Object</b></td>' +
                   '  </tr>';
                   

        for(var i=0; i<obj['_revs_info'].length; i++) {
          text += '<tr>' +
                  '  <td>' +
                  obj['_revs_info'][i]['_id'] +
                  '  </td>' +
                  '  <td>' +
                  obj['_revs_info'][i]['_rev'] +
                  '  </td>' +
                  '  <td>' +
                  toJsonString(obj['_revs_info'][i]) +
                  '  </td>' +
                  '</tr>';
        }

        text += '</table>';

        id.innerHTML = text;
      } catch(e) {
      }
    }
  }

  xmlhttp.send(null);
}

// JSON Library:
toJsonString = function(arg) {
    return toJsonStringArray(arg).join('');
}

toJsonStringArray = function(arg, out) {
    out = out || new Array();
    var u; // undefined

    switch (typeof arg) {
    case 'object':
        if (arg) {
            if (arg.constructor == Array) {
                out.push('[');
                for (var i = 0; i < arg.length; ++i) {
                    if (i > 0)
                        out.push(',\n');
                    toJsonStringArray(arg[i], out);
                }
                out.push(']');
                return out;
            } else if (typeof arg.toString != 'undefined') {
                out.push('{');
                var first = true;
                for (var i in arg) {
                    var curr = out.length; // Record position to allow undo when arg[i] is undefined.
                    if (!first)
                        out.push(',\n');
                    toJsonStringArray(i, out);
                    out.push(':');                    
                    toJsonStringArray(arg[i], out);
                    if (out[out.length - 1] == u)
                        out.splice(curr, out.length - curr);
                    else
                        first = false;
                }
                out.push('}');
                return out;
            }
            return out;
        }
        out.push('null');
        return out;
    case 'unknown':
    case 'undefined':
    case 'function':
        out.push(u);
        return out;
    case 'string':
        out.push('"')
        out.push(arg.replace(/(["\\])/g, '\\$1').replace(/\r/g, '').replace(/\n/g, '\\n'));
        out.push('"');
        return out;
    default:
        out.push(String(arg));
        return out;
    }
}
