import { Component, OnInit, AfterContentInit } from '@angular/core';
import * as d3 from 'd3';
import * as dc from 'dc';
import crossfilter from 'crossfilter2';
import { Dimension } from 'crossfilter2';

@Component({
  selector: 'app-charts-two',
  templateUrl: './charts-two.component.html',
  styleUrls: ['./charts-two.component.css']
})
export class ChartsTwoComponent implements AfterContentInit {

  constructor() {}

  ngAfterContentInit() {

    var worldChart = dc.geoChoroplethChart('#world-chart');
    var regimeChart = dc.pieChart('#regime-chart');
    var pluralismChart = dc.rowChart('#pluralism-chart');
    var governanceChart = dc.rowChart('#gov-chart');
    var politicalChart = dc.rowChart('#political-chart');
    var cultureChart = dc.rowChart('#culture-chart');
    var civilChart = dc.rowChart('#civil-chart');
    var tableChart = dc.dataTable("#table-chart");


    d3.csv('assets/freedomDemocracy_2.csv').then(function (csv) {

      var data = crossfilter(csv);


      var country = data.dimension(function(d){
        return d['COUNTRY'];
      });

      var rank = country.group().reduceSum(function(d){
        return d['RANK_DEMOCRACY'];
      });


      var regime = data.dimension(function(d){
        return d['REGIME'];
      });

      var regimeGroup = regime.group().reduceCount();


      var pluralism = data.dimension(function(d){
        return d['ZONE'];
      });

      var pluralismGroup = pluralism.group().reduceSum(function(d){
        return d['Electoral process and pluralism'];
      });

      var governance = data.dimension(function(d){
        return d['ZONE'];
      });

      var governanceGroup = governance.group().reduceSum(function(d){
        return d['Functioning of government'];
      });


      var political = data.dimension(function(d){
        return d['ZONE'];
      });

      var politicalGroup = political.group().reduceSum(function(d){
        return d['Political participation'];
      });

      var culture = data.dimension(function(d){
        return d['ZONE'];
      });

      var cultureGroup = culture.group().reduceSum(function(d){
        return d['Political culture'];
      });

      var civil = data.dimension(function(d){
        return d['ZONE'];
      });

      var civilGroup = culture.group().reduceSum(function(d){
        return d['Civil liberties'];
      });

      var table = data.dimension(function(d) {
        return parseFloat(d['RANK_DEMOCRACY']);
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

                regimeChart.innerRadius(100)
                    .width(700)
                    .height(400)
                    .dimension(regime)
                    .group(regimeGroup)
                    .colors(d3.scaleOrdinal().range(['#4393c3', '#b2182b', '#d6604d', '#2166ac']))
                    .externalLabels(30)
                    .externalRadiusPadding(70)
                    .drawPaths(true);

                pluralismChart.width(450)
                  .height(200)
                  .margins({top: 20, left: 10, right: 10, bottom: 20})
                  .dimension(pluralism)
                  .group(pluralismGroup)
                  .xAxis().ticks(8);

                governanceChart.width(450)
                  .height(200)
                  .margins({top: 20, left: 10, right: 10, bottom: 20})
                  .dimension(governance)
                  .group(governanceGroup)
                  .xAxis().ticks(8);

                politicalChart.width(450)
                  .height(200)
                  .margins({top: 20, left: 10, right: 10, bottom: 20})
                  .dimension(political)
                  .group(politicalGroup)
                  .xAxis().ticks(8);

                cultureChart.width(450)
                  .height(200)
                  .margins({top: 20, left: 10, right: 10, bottom: 20})
                  .dimension(culture)
                  .group(cultureGroup)
                  .xAxis().ticks(8);

                civilChart.width(450)
                  .height(200)
                  .margins({top: 20, left: 10, right: 10, bottom: 20})
                  .dimension(civil)
                  .group(civilGroup)
                  .xAxis().ticks(8);

                tableChart.dimension(table)
                    .section(function(d){
                      return "Democracy Index";
                    })
                    .size(Infinity)
                    .showSections(false)
                    .columns([
                              function(d){ return d['RANK_DEMOCRACY']; },
                              function(d){ return d['COUNTRY']; },
                              function(d){ return d['ZONE']; },
                              function(d){ return d['Electoral process and pluralism']; },
                              function(d){ return d['Functioning of government']; },
                              function(d){ return d['Political participation']; },
                              function(d){ return d['Political culture']; },
                              function(d){ return d['Civil liberties']; }
                            ]);



        dc.renderAll();

      });

    });
  }
}
