import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DefaultComponent } from './layouts/default/default.component';
import { DashboardComponent } from './modules/dashboard/dashboard.component';
import { PostsComponent } from './modules/posts/posts.component';
import { AnalysisComponent } from './modules/analysis/analysis.component';
import { WorldMapComponent } from './modules/world-map/world-map.component';
import { ChartsOneComponent } from './modules/charts-one/charts-one.component';
import { ChartsTwoComponent } from './modules/charts-two/charts-two.component';
import { ChartsThreeComponent } from './modules/charts-three/charts-three.component';
import { HomeComponent } from './modules/home/home.component';
import { SurveyComponent } from './modules/survey/survey.component';

const routes: Routes = [{
  path: '',
  component: DefaultComponent,
  children: [{
    path: '',
    pathMatch: 'full',
    redirectTo: 'home'
  },{ path:'home',
      component: HomeComponent
  },{ path: 'gkg',
      component: DashboardComponent
  },{
      path: 'events',
      component: PostsComponent
    },{
      path: 'survey',
      component: SurveyComponent
    },{
      path: 'world-map',
      component: WorldMapComponent
    },{
      path: 'charts-one',
      component: ChartsOneComponent
    },{
      path: 'charts-two',
      component: ChartsTwoComponent
    },{
      path: 'charts-three',
      component: ChartsThreeComponent
    }
  ]
}];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
