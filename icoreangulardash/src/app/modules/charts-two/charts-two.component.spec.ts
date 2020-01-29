import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsTwoComponent } from './charts-two.component';

describe('ChartsTwoComponent', () => {
  let component: ChartsTwoComponent;
  let fixture: ComponentFixture<ChartsTwoComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartsTwoComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartsTwoComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
