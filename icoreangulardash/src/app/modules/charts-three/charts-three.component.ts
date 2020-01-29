import { Component, OnInit, AfterContentInit } from '@angular/core';
import * as d3 from 'd3';
import * as dc from 'dc';
import crossfilter from 'crossfilter2';
import { Dimension } from 'crossfilter2';

@Component({
  selector: 'app-charts-three',
  templateUrl: './charts-three.component.html',
  styleUrls: ['./charts-three.component.css']
})
export class ChartsThreeComponent{

  constructor() { }

  ngAfterContentInit() {
    var worldChart = dc.geoChoroplethChart('#world-chart');
    var barChart = dc.barChart('#bar-chart');
    var leiChart = dc.barChart('#lei-chart');
    var schChart = dc.barChart('#sch-chart');
    var filChart = dc.barChart('#fil-chart');
    var lepChart = dc.barChart('#lep-chart');
    var thuChart = dc.barChart('#thu-chart');
    var malChart = dc.barChart('#mal-chart');
    var trpChart = dc.barChart('#trp-chart');
    var tbChart = dc.barChart('#tb-chart');
    var denChart = dc.barChart('#den-chart');
    var tableChart = dc.dataTable("#table-chart");


    d3.csv('assets/freedomPathogens.csv').then(function (csv) {

      var data = crossfilter(csv);

      var country = data.dimension(function(d){
        return d['COUNTRY'];
      });

      var rank = country.group().reduceSum(function(d){
        return d['hpp.9'];
      });

      var magnitude = data.dimension(function(d) {
        if (d['hpp.9'] != null) {
          return d['hpp.9'];
      }});

      var magnitudeGroup = magnitude.group().reduceCount();


      var lei = data.dimension(function(d) {
        if (d['pat.lei'] != null) {
          return d['ZONE'];
      }});

      var leiGroup = lei.group().reduceSum(function(d) {
        return d['pat.lei'];
      });


      var sch = data.dimension(function(d) {
        if (d['pat.sch'] != null) {
          return d['ZONE'];
      }});

      var schGroup = sch.group().reduceSum(function(d) {
        return d['pat.sch'];
      });


      var fil = data.dimension(function(d) {
        if (d['pat.fil'] != null) {
          return d['ZONE'];
      }});

      var filGroup = fil.group().reduceSum(function(d) {
        return d['pat.fil'];
      });


      var lep = data.dimension(function(d) {
        if (d['pat.lep'] != null) {
          return d['ZONE'];
      }});

      var lepGroup = lep.group().reduceSum(function(d) {
        return d['pat.lep'];
      });

      var thu = data.dimension(function(d) {
        if (d['pat.thu'] != null) {
          return d['ZONE'];
      }});

      var thuGroup = thu.group().reduceSum(function(d) {
        return d['pat.thu'];
      });

      var mal = data.dimension(function(d) {
        if (d['pat.mal'] != null) {
          return d['ZONE'];
      }});

      var malGroup = mal.group().reduceSum(function(d) {
        return d['pat.mal'];
      });

      var trp = data.dimension(function(d) {
        if (d['pat.trp'] != null) {
          return d['ZONE'];
      }});

      var trpGroup = trp.group().reduceSum(function(d) {
        return d['pat.trp'];
      });

      var tb = data.dimension(function(d) {
        if (d['pat.tb'] != null) {
          return d['ZONE'];
      }});

      var tbGroup = tb.group().reduceSum(function(d) {
        return d['pat.tb'];
      });

      var den = data.dimension(function(d) {
        if (d['pat.den'] != null) {
          return d['ZONE'];
      }});

      var denGroup = den.group().reduceSum(function(d) {
        return d['pat.den'];
      });


      var table = data.dimension(function(d) {
        return parseFloat(d['RANK_FREEDOM']);
      });



      d3.json('assets/countries.geo.json').then(function (countryJson) {
        barChart.width(700)
                      .height(400)
                      .x(d3.scaleLinear().domain([1, 24]))
                      .xAxisLabel("hpp.9")
                      .yAxisLabel("Count")
                      .dimension(magnitude)
                      .group(magnitudeGroup);

        worldChart.width(900)
            .height(400)
            .dimension(country)
            .group(rank)
            .colors(d3.scaleQuantize().range(['#67001f', '#b2182b','#d6604d','#f4a582','#fddbc7','#d1e5f0',
                  '#92c5de','#4393c3','#2166ac','#053061'].reverse()))
            .colorDomain([0, 24])
            .colorCalculator(function (d) { return d ? worldChart.colors()(d) : '#ccc'; })
            .projection(d3.geoNaturalEarth1()
              .scale(150))
            .overlayGeoJson(countryJson.features, 'country',
                function (d) {
                  return d.properties.name;
                });

        leiChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(lei)
                .group(leiGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.5)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });



        schChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(sch)
                .group(schGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });


        filChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(fil)
                .group(filGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });


        lepChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(lep)
                .group(lepGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });

        thuChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(thu)
                .group(thuGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });

        malChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(mal)
                .group(malGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });

        trpChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(trp)
                .group(trpGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });

        tbChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(tb)
                .group(tbGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });


        denChart.width(450)
                .height(200)
                .x(d3.scaleBand())
                .xUnits(dc.units.ordinal)
                .brushOn(false)
                .xAxisLabel("REGION")
                .yAxisLabel("PATHOGEN PREVALENCE")
                .dimension(den)
                .group(denGroup)
                .barPadding(0.1)
                .gap(1)
                .renderHorizontalGridLines(true)
                .outerPadding(0.05)
                .colors(d3.scaleOrdinal().domain(["positive", "medium", "negative"])
                        .range(['#67001f','#053061','#d1e5f0']))
                .colorAccessor(function(d){

                      if(d.value > 95) {
                          return "positive"
                        }
                      if(d.value > 40 && d.value < 95) {
                          return "medium"
                        }
                      if(d.value < 40) {
                            return "negative"
                          }

                    });



        tableChart.dimension(table)
            .section(function(d){
              return "Pathogen Index";
            })
            .size(Infinity)
            .showSections(false)
            .columns([
                      function(d){ return d['COUNTRY']; },
                      function(d){ return d['ZONE']; },
                      function(d){ return d['hpp.9']; },
                      function(d){ return d['pat.lei']; },
                      function(d){ return d['pat.sch']; },
                      function(d){ return d['pat.fil']; },
                      function(d){ return d['pat.lep']; },
                      function(d){ return d['pat.thu']; },
                      function(d){ return d['pat.mal']; },
                      function(d){ return d['pat.trp']; },
                      function(d){ return d['pat.tb']; },
                      function(d){ return d['pat.den']; }
                    ]);


        dc.renderAll();

      });

    });
  }
}
