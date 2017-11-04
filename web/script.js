/*
 * Copyright (c) 2012 Gerwin Sturm, FoldedSoft e.U. / www.foldedsoft.at
 *
 * Licensed under the Apache License, Version 2.0 (the 'License'); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

(function (global) {
  'use strict';

  var $ = global.$, google = global.google;
  
  function LatitudeTrail() {
    var map, pos, maxMarkers = 1000, locations, numLocations, polyLine, lineHeader, speed = 50, running, stop;
    
    Date.prototype.niceDate = function () {
      var y, m, d, h, min, sec;
      y = this.getFullYear().toString();
      m = (this.getMonth() + 1).toString();
      d  = this.getDate().toString();
      h = this.getHours().toString();
      min = this.getMinutes().toString();
      sec = this.getSeconds().toString();
      return y + '-' + (m[1] ? m : '0' + m[0]) + '-' + (d[1] ? d : '0' + d[0]) + ' ' + (h[1] ? h : '0' + h[0]) + ':' + (min[1] ? min : '0' + min[0]) + ':' + (sec[1] ? sec : '0' + sec[0]);
    };
    var i=0;
    var lastLatLng;
    var distance;
    var latlng=new google.maps.LatLng('33.457','-118.845' )
    function updateLine() {
     
      $('#info').html("Pos: "+pos+"/"+numLocations);
      
      var path = polyLine.getPath() ;
      
      if (path.getLength() > maxMarkers) {
        //path.removeAt(0);
      }
      
      lastLatLng=latlng;
      
      latlng = new google.maps.LatLng(locations[pos].latitudeE7, locations[pos].longitudeE7);
      
      $('#time').html((new Date(locations[pos].timestampMs)).niceDate()+'<br />'+locations[pos].timestampMs);
      
      distance=google.maps.geometry.spherical.computeDistanceBetween(lastLatLng, latlng);
      console.log(distance)
      if (distance > 4000){ 
      //if(locations[pos].newTrack==1){
         updateLine;
         
          polyLine = new google.maps.Polyline({
          strokeColor: 'red',
          strokeOpacity: 1.0,
          strokeWeight: 2,
          map: map,
          geodesic: true
            }
          
          );
        
          path = polyLine.getPath();
          //, latlng;
          
      }
      path.push(latlng);
      
      
      if (map.getBounds() && !map.getBounds().contains(latlng)) {
        map.setCenter(latlng);
      }
      lineHeader.setPosition(latlng);
      $('#pos').val((numLocations - pos) / numLocations * 100);
      pos -= 1;
      if (pos >= 0) {
        
        global.setTimeout(updateLine, (speed) * 1);
        //global.setTimeout(updateLine, 1);
        
      } else {
        running = false;
      }
    }

    map = new google.maps.Map(
      document.getElementById('map_canvas'),
      {
        zoom: 9,
        center: new google.maps.LatLng(33, -118),
        mapTypeId: google.maps.MapTypeId.ROADMAP
      }
    );

    polyLine = new google.maps.Polyline({
      strokeColor: 'red',
      strokeOpacity: 1.0,
      strokeWeight: 2,
      map: map,
      geodesic: true
    });

    lineHeader = new google.maps.Marker({
      icon: {
        path: google.maps.SymbolPath.CIRCLE,
        scale: 5,
        fillColor: 'yellow',
        fillOpacity: 1,
        strokeColor: 'black',
        strokeWeight: 1
      },
      map: map
    });

    if (global.locationJsonData && global.locationJsonData.locations && global.locationJsonData.locations.length > 0) {
      locations = global.locationJsonData.locations;
      numLocations = locations.length;
      pos = numLocations - 1;
      running = true;
      updateLine();
    } else {
      console.log('No JSON Data available, make sure input.js is loaded and sets the window.locationJsonData variable');
    }

    $('#speed').change(function () {
      speed = parseInt($('#speed').val(), 10);
    });
    $('#pos').change(function () {
      pos = numLocations - Math.floor(numLocations * parseInt($('#pos').val(), 10) / 100);
      pos = Math.min(Math.max(0, pos), numLocations - 1);
      if (!running) {
        running = true;
        updateLine();
      }
    }
    
    
  
    );
  }

  $(document).ready(function () {
    global.latitudeTrail = new LatitudeTrail();
  });

}(this));