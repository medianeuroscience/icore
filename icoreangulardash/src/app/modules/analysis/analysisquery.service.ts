import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { HttpHeaders } from '@angular/common/http';


const httpOptions = {
  headers: new HttpHeaders({
    'Content-Type':  'application/json',
  })
}

@Injectable({
  providedIn: 'root'
})

export class QueryAnalysis {

  _url = 'http://localhost:5000';
  _urlview = 'http://localhost:5000/view';
  _urldata = 'http://localhost:5000/data';

  constructor(private _http: HttpClient) { }

  get() {
    return this._http.get<any>(this._urldata, {responseType: 'json'});
  }

}
