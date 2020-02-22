import { Component, OnInit } from '@angular/core';
import { User } from './user';
import { QuerySurvey } from './surveyquery.service';

@Component({
  selector: 'app-survey',
  templateUrl: './survey.component.html',
  styleUrls: ['./survey.component.css']
})

export class SurveyComponent implements OnInit {

  rand_numb = Math.floor((Math.random() + 1) * 100000000000).toString();
  rand_lett = Math.floor(((Math.random() + 1) * 100000000000)).toString(36).substring(2);
  rand_lett2 = Math.floor(((Math.random() + 1) * 100000000000)).toString(36).substring(2);

  rand_lett3 = this.rand_lett.concat(this.rand_numb);

  queryID = this.rand_lett3.concat(this.rand_lett2);


  userModel = new User('', '', '', '', '', this.queryID);
  userSubmitted = '';

  constructor(private _querySurvey: QuerySurvey) { }


  ngOnInit() {
  }

  Submit() {
    this._querySurvey.query(this.userModel)
      .subscribe(
        data => console.log('Success!', data),
        error => console.log('Error!', error)
      );

    this._querySurvey.get()
      .subscribe(
        data => console.log('getSuccess!', data),
        error => console.log('getError!', error)
      );

    this.userSubmitted = 'Submitted!';
  }

}
