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

export class QueryEvent {
  _url = 'http://169.231.235.236:5000/api/usereventsp';
  _urlview = 'http://169.231.235.236:5000/api/usereventsg';

  constructor(private _http: HttpClient) { }

  query(user: User){
    return this._http.post<any>(this._url, user, httpOptions);
  }

  get(){
    return this._http.get(this._urlview, {responseType: 'text'});
  }
}
