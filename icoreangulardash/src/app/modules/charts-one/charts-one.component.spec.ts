import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsOneComponent } from './charts-one.component';

describe('ChartsOneComponent', () => {
  let component: ChartsOneComponent;
  let fixture: ComponentFixture<ChartsOneComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartsOneComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartsOneComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
