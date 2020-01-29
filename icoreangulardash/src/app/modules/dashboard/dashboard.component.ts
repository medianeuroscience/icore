import { Component, OnInit } from '@angular/core';
import { User } from './user';
import { QueryGKG } from './gkgquery.service';
import country_data from 'src/assets/countries_file.json';
import themes_data from 'src/assets/themes.json';
import { Router } from '@angular/router';


@Component({
  selector: 'app-dashboard',
  templateUrl: './dashboard.component.html',
  styleUrls: ['./dashboard.component.css']
})
export class DashboardComponent implements OnInit {

  countries: any = country_data;
  topics: any = themes_data;

  issues = ['Civil Liberties', 'Domestic Economy', 'Drugs', 'Education', 'Election Fraud', 'Environment', 'Gun Control',
  'Health Care', 'Immigration/Refugees', 'International Relations', 'Media/Internet', 'Military', 'Party-politics',
  'Police-System', 'Racism', 'Taxes', 'Terrorism', 'Trade', 'Unemployment'];

  userModel = new User('', '', '', 'default', '', '', 'default', '', 'default', '', '')
  userSubmitted = ''

  constructor(private _queryGKG: QueryGKG, private _router: Router){}

  ngOnInit() {
  }

  Submit(){
    this._queryGKG.query(this.userModel)
      .subscribe(
        data => console.log('Success!', data),
        error => console.log('Error!', error)
      )

    this._queryGKG.get()
      .subscribe(
        data => console.log('getSuccess!', data),
        error => console.log('getError!', error)
      )

    this._router.navigate(['/', 'survey']);

    this.userSubmitted = 'Submitted!'
    console.log('working');
  }

}
