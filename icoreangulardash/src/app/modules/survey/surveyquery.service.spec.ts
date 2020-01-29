import { TestBed } from '@angular/core/testing';

import { SurveyqueryService } from './surveyquery.service';

describe('SurveyqueryService', () => {
  beforeEach(() => TestBed.configureTestingModule({}));

  it('should be created', () => {
    const service: SurveyqueryService = TestBed.get(SurveyqueryService);
    expect(service).toBeTruthy();
  });
});
