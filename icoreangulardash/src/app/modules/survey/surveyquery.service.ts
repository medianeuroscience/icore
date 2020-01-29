import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { User } from './user';
import { HttpHeaders } from '@angular/common/http';

const httpOptions = {
  headers: new HttpHeaders({
    'Content-Type':  'application/json',
  })
}

@Injectable({
  providedIn: 'root'
})

export class QuerySurvey {
  _url = 'http://localhost:5000/api/usersurveysp';
  _urlview = 'http://localhost:5000/api/usersurveysg';

  constructor(private _http: HttpClient) { }

  query(user: User){
    return this._http.post<any>(this._url, user, httpOptions);
  }

  get(){
    return this._http.get(this._urlview, {responseType: 'text'});
  }

}
