import { Component, ViewEncapsulation, OnInit } from '@angular/core';
import { QueryAnalysis } from './analysisquery.service';
import * as d3 from "d3";
/*import * as d3 from "d3-selection";*/
import * as d3Scale from "d3-scale";
import * as d3Array from "d3-array";
import * as d3Axis from "d3-axis";

import { STATISTICS } from "./Statistics";


@Component({
  selector: 'app-analysis',
  encapsulation: ViewEncapsulation.None,
  templateUrl: './analysis.component.html',
  styleUrls: ['./analysis.component.css']
})


export class AnalysisComponent implements OnInit {

  private width: number;
  private height: number;
  private margin = { top: 20, right: 20, bottom: 30, left: 40 };

  private x: any;
  private y: any;
  private svg: any;
  private g: any;


  constructor(private _queryAnalysis: QueryAnalysis) { }

  ngOnInit() {

    this._queryAnalysis.get()
      .subscribe(data => {

        this.svg = d3.select("#first");
        this.width = +this.svg.attr("width") - this.margin.left - this.margin.right;
        this.height =
          +this.svg.attr("height") - this.margin.top - this.margin.bottom;
        this.g = this.svg
          .append("g")
          .attr(
            "transform",
            "translate(" + this.margin.left + "," + this.margin.top + ")"
          );

        this.x = d3Scale
          .scaleBand()
          .rangeRound([0, this.width])
          .padding(0.1);
        this.y = d3Scale.scaleLinear().rangeRound([this.height, 0]);
        this.x.domain(data.map(d => d.letter));
        this.y.domain([0, d3Array.max(data, d => d.frequency)]);

        this.g
          .append("g")
          .attr("class", "axis axis--x")
          .attr("transform", "translate(0," + this.height + ")")
          .call(d3Axis.axisBottom(this.x));

        this.g
          .append("g")
          .attr("class", "axis axis--y")
          .call(d3Axis.axisLeft(this.y).ticks(10, "%"))
          .append("text")
          .attr("class", "axis-title")
          .attr("transform", "rotate(-90)")
          .attr("y", -20)
          .attr("x", -200)
          .attr("dy", "0.71em")
          .text("Frequency");

        this.g
          .selectAll(".bar")
          .data(data)
          .enter()
          .append("rect")
          .attr("class", "bar")
          .attr("x", d => this.x(d.letter))
          .attr("y", d => this.y(d.frequency))
          .attr("width", this.x.bandwidth())
          .attr("height", d => this.height - this.y(d.frequency));
      });

      this._queryAnalysis.get()
        .subscribe(data2 => {

          this.svg = d3.select("#second");
          this.width = +this.svg.attr("width") - this.margin.left - this.margin.right;
          this.height =
            +this.svg.attr("height") - this.margin.top - this.margin.bottom;
          this.g = this.svg
            .append("g")
            .attr(
              "transform",
              "translate(" + this.margin.left + "," + this.margin.top + ")"
            );

          this.x = d3Scale
            .scaleBand()
            .rangeRound([0, this.width])
            .padding(0.1);
          this.y = d3Scale.scaleLinear().rangeRound([this.height, 0]);
          this.x.domain(data2.map(d => d.letter));
          this.y.domain([0, d3Array.max(data2, d => d.frequency)]);

          this.g
            .append("g")
            .attr("class", "axis axis--x")
            .attr("transform", "translate(0," + this.height + ")")
            .call(d3Axis.axisBottom(this.x));

          this.g
            .append("g")
            .attr("class", "axis axis--y")
            .call(d3Axis.axisLeft(this.y).ticks(10, "%"))
            .append("text")
            .attr("class", "axis-title")
            .attr("transform", "rotate(-90)")
            .attr("y", -20)
            .attr("x", -200)
            .attr("dy", "0.71em")
            .text("Frequency");

          this.g
            .selectAll(".bar")
            .data(data2)
            .enter()
            .append("rect")
            .attr("class", "bar")
            .attr("x", d => this.x(d.letter))
            .attr("y", d => this.y(d.frequency))
            .attr("width", this.x.bandwidth())
            .attr("height", d => this.height - this.y(d.frequency));
        });


/*    this.initSvg(); */
/*    this.initAxis(); */
/*    this.drawAxis(); */
/*    this.drawBars(); */
  }


  private initSvg() {
    this.svg = d3.select("#second");
    this.width = +this.svg.attr("width") - this.margin.left - this.margin.right;
    this.height =
      +this.svg.attr("height") - this.margin.top - this.margin.bottom;
    this.g = this.svg
      .append("g")
      .attr(
        "transform",
        "translate(" + this.margin.left + "," + this.margin.top + ")"
      );
  }

  private initAxis() {
      this.x = d3Scale
        .scaleBand()
        .rangeRound([0, this.width])
        .padding(0.1);
      this.y = d3Scale.scaleLinear().rangeRound([this.height, 0]);
      this.x.domain(STATISTICS.map(d => d.letter));
      this.y.domain([0, d3Array.max(STATISTICS, d => d.frequency)]);
    }

  private drawAxis() {
     this.g
       .append("g")
       .attr("class", "axis axis--x")
       .attr("transform", "translate(0," + this.height + ")")
       .call(d3Axis.axisBottom(this.x));
     this.g
       .append("g")
       .attr("class", "axis axis--y")
       .call(d3Axis.axisLeft(this.y).ticks(10, "%"))
       .append("text")
       .attr("class", "axis-title")
       .attr("transform", "rotate(-90)")
       .attr("y", -20)
       .attr("x", -200)
       .attr("dy", "0.71em")
       .text("Frequency");
  }

  private drawBars() {
     this.g
       .selectAll(".bar")
       .data(STATISTICS)
       .enter()
       .append("rect")
       .attr("class", "bar")
       .attr("x", d => this.x(d.letter))
       .attr("y", d => this.y(d.frequency))
       .attr("width", this.x.bandwidth())
       .attr("height", d => this.height - this.y(d.frequency));
   }
 }
