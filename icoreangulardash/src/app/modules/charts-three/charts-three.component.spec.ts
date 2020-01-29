import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ChartsThreeComponent } from './charts-three.component';

describe('ChartsThreeComponent', () => {
  let component: ChartsThreeComponent;
  let fixture: ComponentFixture<ChartsThreeComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ChartsThreeComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ChartsThreeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
