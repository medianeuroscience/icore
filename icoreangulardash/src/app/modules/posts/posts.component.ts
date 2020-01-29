import { Component, OnInit } from '@angular/core';
import { User } from './user';
import { QueryEvent } from './eventquery.service';
import country_data from 'src/assets/countries_file.json';
import { Router } from '@angular/router';


@Component({
  selector: 'app-posts',
  templateUrl: './posts.component.html',
  styleUrls: ['./posts.component.css']
})
export class PostsComponent implements OnInit {

  countries: any = country_data;
  events = ['Consult', 'Make Public Statement', 'Appeal', 'Disapprove', 'Fight',
  'Engage in Diplomatic Cooperation', 'Coerce', 'Express Intent to Cooperate',
  'Yield', 'Provide Aid', 'Reject', 'Investigate', 'Assault', 'Engage in Material Cooperation',
  'Threaten', 'Demand', 'Reduce Relations', 'Protest', 'Exhibit Force Posture',
  'Use Conventional Mass Violence'];

  userModel = new User('', '', 'default', 'default', '', '', '')
  userSubmitted = ''

  constructor(private _queryEvent: QueryEvent, private _router: Router) { }

  ngOnInit() {
  }

  Submit(){
    this._queryEvent.query(this.userModel)
      .subscribe(
        data => console.log('Success!', data),
        error => console.log('Error!', error)
      )

    this._queryEvent.get()
      .subscribe(
        data => console.log('getSuccess!', data),
        error => console.log('getError!', error)
      )

    this._router.navigate(['/', 'survey']);

    this.userSubmitted = 'Submitted!'
    console.log('working');
  }

}
