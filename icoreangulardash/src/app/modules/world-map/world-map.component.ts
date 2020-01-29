import { Component, OnInit } from '@angular/core';
// import country_shapes from 'src/assets/polygons.json';
import * as d3 from 'd3';
// import * as t from 'topojson-client';

@Component({
  selector: 'app-world-map',
  templateUrl: './world-map.component.html',
  styleUrls: ['./world-map.component.css']
})
export class WorldMapComponent implements OnInit {

  constructor() { }

  ngOnInit() {

    var mapWidth = 1000,
        mapHeight = 510

    var margin = {top: 10, right: 30, bottom: 30, left: 40},
    lineWidth = 460 - margin.left - margin.right,
    lineHeight = 400 - margin.top - margin.bottom;

    var canvas = d3.select('#world').append('svg')
      .attr('width', mapWidth)
      .attr('height', mapHeight)

    var lineSvg = d3.select("#line")
    .append("svg")
      .attr("width", lineWidth + margin.left + margin.right)
      .attr("height", lineHeight + margin.top + margin.bottom)
    .append("g")
      .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");


    d3.json("assets/countries.geo.json").then(function (data) {

        var group = canvas.selectAll('g')
            .data(data.features)
            .enter()
            .append('g')


        var projection = d3.geoNaturalEarth1()
                  .scale(200)
                  .center([10.439235,10.830666])
                  .translate([mapWidth / 2, mapHeight / 2]);

        var path = d3.geoPath().projection(projection);


        var areas = group.append('path')
            .attr('d', path)
            .attr('fill','gray')


        d3.csv("assets/us_events_2018.csv").then(function (events) {


            var data_location = [];
            var data_tone = [];
            var data_et = [];
            var data_articles = [];
            var data_day = [];


            events.forEach(function(d){
              var data_mini = [];
              var long = parseFloat(d['action_geo_long']);
              var lat = parseFloat(d['action_geo_lat']);
              var tone = parseFloat(d['event_tone_avg']);
              var event_type = d['event_type'];
              var num_articles = parseFloat(d['num_articles']);
              var event_day = parseFloat(d['event_day']);
              data_mini.push(long, lat);
              data_location.push(data_mini);
              data_tone.push(tone);
              data_et.push(event_type);
              data_articles.push(num_articles);
              data_day.push(event_day);


            });


            //var topData = data_location.slice(0, 200);
            var newData = [];
            //var topData = data_location.slice(0, 1000);
            //var topData = data_location.slice(0, 1000);
            //var topData = data_location.slice(0, 1000);
            //var topData = data_location.slice(0, 1000);

            for (var i = 150; i < data_tone.length; i++) {
              newData.push(data_tone[i]);
              //Do something
            }


            function init_draw() {
              var random = data_location.slice(0, 150);
              //console.log(random);

              var circles = group.select(".dataCircle")
                                .data(random)
                                .enter()
                            		.append("circle")
                            		.attr("cx", function (d) { return projection(d)[0]; })
                            		.attr("cy", function (d) { return projection(d)[1]; })
                            		.attr("r", "2px")
                                .attr("fill", "red")
                                .attr("opacity", .1)
                                .exit()
}

            function later_draw() {
              var random = data_location.slice(0, 150);
              var random3 = random[Math.floor(Math.random() * random.length)];
              var random2 = newData[Math.floor(Math.random() * newData.length)];

              console.log(random);


              var circles2 = group.selectAll(".dataCircle2")
                                .data([random3])
                                .enter()
                                .append("circle")
                                .attr("cx", function (d) { return projection(d)[0]; })
                                .attr("cy", function (d) { return projection(d)[1]; })
                                .attr("fill", "red")
                                .attr("opacity", .1)
                                .attr("r", "2px")
                                .transition()
                                  .duration(2000)
                                  .ease(d3.easeSin, 2)
                                  .attr("r", "46px")
                                .transition()
                                  .duration(500)
                                  .ease(d3.easeSin, 2)
                                  .attr("r", "2px")

              var lines = lineSvg.append("circle")
                                  .transition()
                                  .duration(2000)
                                  .attr("cx", 30)
                                  .attr("cy", 30)
                                  .attr("r", 20)
                                  .transition()
                                  .duration(2000)
                                  .attr("r", random2)


            }


          init_draw();

          setInterval(function() {
            later_draw();
          }, 5000);
        });

    });

}
}
