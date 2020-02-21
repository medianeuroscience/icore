import { Component, OnInit, OnDestroy } from '@angular/core';
// import country_shapes from 'src/assets/polygons.json';
import * as d3 from 'd3';

// import * as t from 'topojson-client';

@Component({
  selector: 'app-world-map',
  templateUrl: './world-map.component.html',
  styleUrls: ['./world-map.component.css']
})
export class WorldMapComponent implements OnInit {

  private xt: any;

  constructor() {
  }

  ngOnInit() {


    var mapWidth = 900,
        mapHeight = 510

    var margin = {top: 20, right: 20, bottom: 30, left: 40 },
    lineWidth = 1700 - margin.left - margin.right,
    lineHeight = 300 - margin.top - margin.bottom,
    barWidth = 700 - margin.left - margin.right,
    barHeight = 400 - margin.top - margin.bottom;

    var canvas = d3.select('#world').append('svg')
      .attr('width', mapWidth)
      .attr('height', mapHeight)

      // append the svg object to the body of the page
    var lineSvg = d3.select("#line")
      .append("svg")
        .attr("width", lineWidth + margin.left + margin.right)
        .attr("height", lineHeight + margin.top + margin.bottom)
      .append("g")
        .attr("transform",
              "translate(" + margin.left + "," + margin.top + ")");

    var numberSvg = d3.select("#number")
      .append("svg")
        .attr("width", barWidth + margin.left + margin.right + 100)
        .attr("height", barHeight + margin.top + margin.bottom + 200)
      .append("g")
        .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");

        // parse the date / time
    var parseTime = d3.timeParse("%Y-%m-%d");



    d3.json("assets/countries.geo.json").then(data => {

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


        d3.csv("http://169.231.235.236:5000/api/eventsDash").then(events => {


            var data_location = [];
            var data_tone = [];
            var data_et = [];
            var data_articles = [];
            var data_day = [];

            var line_result = [];
            var bar_result = [];
            var lineData = [];


            events.forEach(d => {
              var data_mini = [];
              var long = parseFloat(d['action_geo_long']);
              var lat = parseFloat(d['action_geo_lat']);
              var tone = parseFloat(d['event_tone_avg']);
              var event_type = d['event_root_code'];
              var num_articles = parseFloat(d['num_articles']);
              var event_day = d['event_day'];

              data_mini.push(long, lat);

              lineData.push({
                date: event_day,
                tone: tone,
                articles: num_articles,
                location: data_mini,
                event_type: event_type
              });


            });

            //console.log(data_et);




            var lineCounts = d3.nest()
                      .key(d => { return d.date; })
                      .rollup(v => { return d3.mean(v, d => { return d.tone; }); })
                      .entries(lineData);

            var barCounts = d3.nest()
                      .key(d => { return d.event_type; })
                      .rollup(v => { return d3.sum(v, d => { return d.articles; }); })
                      .entries(lineData);


            lineCounts.forEach(d => {
              line_result.push({
                date: parseTime(d.key),
                tone: d.value
              });
            });

            barCounts.forEach(d =>{
              bar_result.push({
                event_type: d.key,
                articles: parseFloat(d.value)
              });
            });


            function sortByDateAscending(a, b) {
                // Dates will be cast to numbers automagically:
                return a.date - b.date;
            }

            function sortByArticlesAscending(a, b) {
                // Dates will be cast to numbers automagically:
                return b.articles - a.articles;
            }


            var line_resultSorted_init = line_result.sort(sortByDateAscending);
            var line_resultSorted_init = line_resultSorted_init.slice(0, 20);

            var bar_resultSorted_init = bar_result.slice(0, 20);
            var bar_resultSorted_init = bar_resultSorted_init.sort(sortByArticlesAscending);


            console.log(bar_resultSorted_init);



            //console.log(barChart_init);
            //console.log(resultSorted_init);

                    // Add X axis --> it is a date format
            var x = d3.scaleTime()
              .domain(d3.extent(line_resultSorted_init, d => { return d.date; }))
              .range([ 0, lineWidth ]);

            lineSvg.append("g")
              .attr("transform", "translate(0," + lineHeight + ")")
              .call(d3.axisBottom(x));

            lineSvg.append("text")
                    .attr("transform",
                          "translate(" + (lineWidth/2) + " ," +
                                         (lineHeight + margin.top + 20) + ")")
                    .style("text-anchor", "middle")
                    .style("font-size", "16px")
                    .text("Date");

                        // Add Y axis
            var y = d3.scaleLinear()
              .domain([d3.min(line_resultSorted_init, d => { return +d.tone; }) - 1, d3.max(line_resultSorted_init, d => { return +d.tone; })])
              .range([ lineHeight, 0 ]);

            lineSvg.append("g")
              .call(d3.axisLeft(y));


                      // Add the line
            lineSvg.append("path")
              .datum(line_resultSorted_init)
              .attr("fill", "none")
              .attr("stroke", "purple")
              .attr("stroke-width", 1.5)
              .attr("opacity", .5)
              .attr("d", d3.line()
                .x(d => { return x(d.date) })
                .y(d => { return y(d.tone) })
                )


            var bar_x = d3.scaleBand()
              .rangeRound([0, barWidth]);

            bar_x.domain(bar_resultSorted_init.map(d => d.event_type));

            numberSvg
              .append("g")
              .attr("class", "axis axis--x")
              .attr("transform", "translate(0," + barHeight + ")")
              .call(d3.axisBottom(bar_x))
              .selectAll("text")
              .attr("y", 0)
              .attr("x", 9)
              .attr("dy", ".35em")
              .attr("transform", "rotate(90)")
              .style("text-anchor", "start");

            var bar_y = d3.scaleLinear()
              .rangeRound([barHeight, 0]);

            bar_y.domain([d3.min(bar_resultSorted_init, d => d.articles), d3.max(bar_resultSorted_init, d => d.articles)]);


            numberSvg
              .append("g")
              .attr("class", "axis axis--y")
              .call(d3.axisLeft(bar_y).ticks(10))
              .append("text")
              .attr("transform", "rotate(-90)")
              .text("Number of Articles");

            numberSvg
              .selectAll(".bar")
              .data(bar_resultSorted_init)
              .enter()
              .append("rect")
              .attr("class", "bar")
              .attr("x", d => bar_x(d.event_type))
              .attr("y", d => bar_y(d.articles))
              .attr("width", bar_x.bandwidth())
              .attr("height", d => barHeight - bar_y(d.articles));


            //console.log(result);


            function init_draw() {
              var events_circles = lineData.slice(0, 15);

              var circles = group.select(".dataCircle")
                                .data(events_circles.filter(d => { return d.articles > 6; }))
                                .enter()
                            		.append("circle")
                            		.attr("cx", d => { return projection(d.location)[0]; })
                            		.attr("cy", d => { return projection(d.location)[1]; })
                            		.attr("r",  "5px")
                                .attr("fill", "gold")
                                .attr("opacity", .3)
                                .exit()


              var circles = group.select(".dataCircle")
                                .data(events_circles.filter(d =>{ return d.articles > 6;  }))
                                .enter()
                            		.append("text")
                            		.attr("x", d => { return projection(d.location)[0]; })
                            		.attr("y", d => { return projection(d.location)[1]; })
                                .style("font-size", "5px")
                                .style("text-anchor", "middle")
                                .exit()

              }

            function later_draw() {
              //var random = data_location.slice(0, 150);
              //var random3 = random[Math.floor(Math.random() * random.length)];

              start++;
              end++;

              var line_resultSorted_later = line_result.sort(sortByDateAscending);
              var line_resultSorted_later = line_resultSorted_later.slice(start, end);

              var bar_resultSorted_later = bar_result.slice(start, end);
              var bar_resultSorted_later = bar_resultSorted_later.sort(sortByArticlesAscending);

              var circle_location_later = lineData.slice(start, end);
              var circle_location_later_2 = circle_location_later.slice(-1)[0];

              console.log(bar_resultSorted_later);


              lineSvg.selectAll('path').remove();
              lineSvg.selectAll('g').remove();
              lineSvg.selectAll('text').remove();

              numberSvg.selectAll('path').remove();
              numberSvg.selectAll('g').remove();
              numberSvg.selectAll('rect').remove();
              numberSvg.selectAll('text').remove();



              var circles2 = group.selectAll(".dataCircle2")
                                .data([circle_location_later_2])
                                .enter()
                                .append("circle")
                                .attr("cx", d => { return projection(d.location)[0]; })
                                .attr("cy", d => { return projection(d.location)[1]; })
                                .attr("fill", "gold")
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

              // Add X axis --> it is a date format
              var x = d3.scaleTime()
                .domain(d3.extent(line_resultSorted_later, d => { return d.date; }))
                .range([ 0, lineWidth ]);

              lineSvg.append("g")
                .attr("transform", "translate(0," + lineHeight + ")")
                .call(d3.axisBottom(x));

              lineSvg.append("text")
                      .attr("transform",
                            "translate(" + (lineWidth/2) + " ," +
                                           (lineHeight + margin.top + 20) + ")")
                      .style("text-anchor", "middle")
                      .style("font-size", "16px")
                      .text("Date");

                          // Add Y axis
              var y = d3.scaleLinear()
                .domain([d3.min(line_resultSorted_later, d => { return +d.tone; }) - 1, d3.max(line_resultSorted_later, d => { return +d.tone; })])
                .range([ lineHeight, 0 ]);

              lineSvg.append("g")
                .call(d3.axisLeft(y));

              lineSvg.append("text")
                    .attr("transform", "rotate(-90)")
                    .attr("y", 0 - margin.left)
                    .attr("x",0 - (lineHeight / 2))
                    .attr("dy", "1em")
                    .style("text-anchor", "middle")
                    .style("font-size", "12px")
                    .text("Tone");


                        // Add the line
              lineSvg.append("path")
                .datum(line_resultSorted_later)
                .attr("fill", "none")
                .attr("stroke", "purple")
                .attr("stroke-width", 1.5)
                .attr("opacity", .5)
                .attr("d", d3.line()
                  .x(d => { return x(d.date) })
                  .y(d => { return y(d.tone) })
                  )

              var bar_x = d3.scaleBand()
                .rangeRound([0, barWidth]);

              bar_x.domain(bar_resultSorted_later.map(d => d.event_type));

              numberSvg
                .append("g")
                .attr("class", "axis axis--x")
                .attr("transform", "translate(0," + barHeight + ")")
                .call(d3.axisBottom(bar_x))
                .selectAll("text")
                .attr("y", 0)
                .attr("x", 9)
                .attr("dy", ".35em")
                .attr("transform", "rotate(90)")
                .style("text-anchor", "start");

              var bar_y = d3.scaleLinear()
                .rangeRound([barHeight, 0]);

              bar_y.domain([d3.min(bar_resultSorted_later, d => d.articles), d3.max(bar_resultSorted_later, d => d.articles)]);


              numberSvg
                .append("g")
                .attr("class", "axis axis--y")
                .call(d3.axisLeft(bar_y).ticks(10))
                .append("text")
                .attr("transform", "rotate(-90)")
                .text("Number of Articles");

              numberSvg
                .selectAll(".bar")
                .data(bar_resultSorted_later)
                .enter()
                .append("rect")
                .attr("class", "bar")
                .attr("x", d => bar_x(d.event_type))
                .attr("y", d => bar_y(d.articles))
                .attr("width", bar_x.bandwidth())
                .attr("height", d => barHeight - bar_y(d.articles));



            }


          init_draw();

          var start = 0;
          var end = 20;
          var count = 0;

          this.xt = setInterval(() => {

          later_draw();
          if(count > 10) clearInterval(this.xt);
          count++;

        }, 5000);


        });

    });

}


ngOnDestroy() {
     var xt = this.xt;
     clearInterval(xt);
 }

}
