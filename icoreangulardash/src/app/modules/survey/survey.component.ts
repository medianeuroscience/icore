import { Component, OnInit } from '@angular/core';
import { User } from './user';
import { QuerySurvey } from './surveyquery.service';

@Component({
  selector: 'app-survey',
  templateUrl: './survey.component.html',
  styleUrls: ['./survey.component.css']
})

export class SurveyComponent implements OnInit {

  userModel = new User('', '', '', '', '')
  userSubmitted = ''

  constructor(private _querySurvey: QuerySurvey) { }

  ngOnInit() {
  }

  Submit(){
    this._querySurvey.query(this.userModel)
      .subscribe(
        data => console.log('Success!', data),
        error => console.log('Error!', error)
      )

    this._querySurvey.get()
      .subscribe(
        data => console.log('getSuccess!', data),
        error => console.log('getError!', error)
      )

    this.userSubmitted = 'Submitted!';
  }

}
