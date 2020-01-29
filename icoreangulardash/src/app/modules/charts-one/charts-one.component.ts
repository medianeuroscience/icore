import { Component, OnInit, AfterContentInit } from '@angular/core';
import * as d3 from 'd3';
import * as dc from 'dc';
import crossfilter from 'crossfilter2';
import { Dimension } from 'crossfilter2';


@Component({
  selector: 'app-charts-one',
  templateUrl: './charts-one.component.html',
  styleUrls: ['./charts-one.component.css']
})


export class ChartsOneComponent {

  constructor() {
  }

  ngAfterContentInit() {

    var scatterChart = dc.scatterPlot('#scatter-chart');
    var worldChart = dc.geoChoroplethChart('#world-chart');
    var zoneChart = dc.pieChart('#zone-chart');
    var govChart = dc.pieChart('#gov-chart');
    var incomeChart = dc.pieChart('#income-chart');
    var numbChart = dc.numberDisplay('#number-chart');
    var tableChart = dc.dataTable("#table-chart");

    d3.csv('assets/FPGR.csv').then(function(csv){

      console.log(csv);

      var data = crossfilter(csv);

      var country = data.dimension(function(d){
        return d['COUNTRY'];
      });

      var rank = country.group().reduceSum(function(d){
        return d['RANK_FREEDOM'];
      });

      var zone = data.dimension(function(d){
        return d['ZONE'];
      });

      var zoneGroup = zone.group().reduceCount();


      var gov = data.dimension(function(d){
        return d['REGIME'];
      });

      var govGroup = gov.group().reduceCount();


      var income = data.dimension(function(d){
        return d['IncomeGroup'];
      });

      var incomeGroup = income.group().reduceCount();


      var numberZone = data.dimension(function(d){
        return d['ZONE'];
      });

      var numberGroup = numberZone.groupAll().reduceSum(function(d){
        return parseFloat(d['ABUSE_SCORE']);
      });

      var scatter = data.dimension(function(d) {
        return [parseFloat(d['PROGRESS_FREEDOM']), parseFloat(d['RANK_FREEDOM'])];
      });

      var scatterGroup = scatter.group().reduceCount();

      var table = data.dimension(function(d) {
        return parseFloat(d['RANK_FREEDOM']);
      });


      d3.json('assets/countries.geo.json').then(function (countryJson) {

              worldChart.width(900)
                  .height(430)
                  .dimension(country)
                  .group(rank)
                  .colors(d3.scaleQuantize().range(['#67001f', '#b2182b','#d6604d','#f4a582','#fddbc7','#d1e5f0',
                        '#92c5de','#4393c3','#2166ac','#053061'].reverse()))
                  .colorDomain([0, 180])
                  .colorCalculator(function (d) { return d ? worldChart.colors()(d) : '#ccc'; })
                  .projection(d3.geoNaturalEarth1()
                    .scale(160))
                  .overlayGeoJson(countryJson.features, 'country',
                      function (d) {
                        return d.properties.name;
                      });

              scatterChart.width(700)
                  .height(430)
                  .x(d3.scaleLinear().domain([-40, 45]))
                  .y(d3.scaleLinear().domain([0, 200]))
                  .xAxisLabel("CHANGE IN RANK")
                  .yAxisLabel("RANK 2019")
                  .symbolSize(6)
                  .clipPadding(10)
                  .dimension(scatter)
                  .group(scatterGroup);

              zoneChart.innerRadius(40)
                  .width(300)
                  .dimension(zone)
                  .group(zoneGroup);


              govChart.innerRadius(40)
                  .width(300)
                  .dimension(gov)
                  .group(govGroup);

              numbChart.formatNumber(d3.format('.1d'))
                  .valueAccessor(function(d){
                    return d;
                  })
                  .group(numberGroup);


              incomeChart.innerRadius(40)
                  .width(300)
                  .dimension(income)
                  .group(incomeGroup);


              tableChart.dimension(table)
                  .section(function(d){
                    return "Press Index";
                  })
                  .size(Infinity)
                  .showSections(false)
                  .columns([
                      function(d){ return d['RANK_FREEDOM']; },
                      function(d){ return d['COUNTRY']; },
                      function(d){ return d['ZONE']; },
                      function(d){ return d['REGIME']; },
                      function(d){ return d['IncomeGroup']; },
                      function(d){ return parseFloat(d['ABUSE_SCORE']); },
                      function(d){ return d['PROGRESS_FREEDOM']; }
                    ]);

      dc.renderAll();

    });
  });

}
}
