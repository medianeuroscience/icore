import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';
import { DefaultComponent } from './default.component';
import { DashboardComponent } from 'src/app/modules/dashboard/dashboard.component';
import { RouterModule } from '@angular/router';
import { PostsComponent } from 'src/app/modules/posts/posts.component';
import { SharedModule } from 'src/app/shared/shared.module';
import { MatSidenavModule, MatDividerModule } from '@angular/material';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { AnalysisComponent } from 'src/app/modules/analysis/analysis.component';
import { WorldMapComponent } from 'src/app/modules/world-map/world-map.component';
import { ChartsOneComponent } from 'src/app/modules/charts-one/charts-one.component';
import { ChartsTwoComponent } from 'src/app/modules/charts-two/charts-two.component';
import { ChartsThreeComponent } from 'src/app/modules/charts-three/charts-three.component';
import { HomeComponent } from 'src/app/modules/home/home.component';
import { SurveyComponent } from 'src/app/modules/survey/survey.component';

import { MatFormFieldModule, MatInputModule } from '@angular/material';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatButtonModule} from '@angular/material/button';
import { MatSelectModule} from '@angular/material/select';
import {MatCardModule} from '@angular/material/card';

import { HttpClientModule } from  '@angular/common/http';



@NgModule({
  declarations: [
    DefaultComponent,
    DashboardComponent,
    PostsComponent,
    AnalysisComponent,
    WorldMapComponent,
    ChartsOneComponent,
    ChartsTwoComponent,
    ChartsThreeComponent,
    HomeComponent,
    SurveyComponent
  ],
  imports: [
    CommonModule,
    RouterModule,
    SharedModule,
    MatSidenavModule,
    MatDividerModule,
    MatFormFieldModule,
    MatInputModule,
    MatGridListModule,
    FormsModule,
    ReactiveFormsModule,
    MatButtonModule,
    MatSelectModule,
    MatCardModule,
    HttpClientModule
  ]
})
export class DefaultModule { }
